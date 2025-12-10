//! Local filesystem storage backend
//!
//! This backend stores all data on the local filesystem under a base directory.
//! It's the default implementation and works immediately without any external dependencies.
//!
//! ## Directory Structure
//!
//! ```text
//! /data/featureduck/
//!   ├─ incremental/
//!   │   ├─ user_features/
//!   │   │   ├─ state_v1.duckdb
//!   │   │   ├─ state_v2.duckdb
//!   │   │   └─ metadata.json
//!   │   └─ product_features/
//!   ├─ features/          (Delta Lake tables)
//!   └─ registry/          (DuckDB registry)
//! ```

use async_trait::async_trait;
use std::path::{Path, PathBuf};
use tokio::fs;

use super::{ObjectMetadata, StorageBackend};
use crate::{Error, Result};

/// Local filesystem storage backend
///
/// Stores everything under a base directory. Remote keys are treated as relative paths
/// from the base directory.
///
/// # Thread Safety
/// Safe to use from multiple threads. File operations use atomic operations where possible.
pub struct LocalStorageBackend {
    base_path: PathBuf,
}

impl LocalStorageBackend {
    /// Create new local storage backend
    ///
    /// # Arguments
    /// * `base_path` - Base directory for all storage (created if doesn't exist)
    ///
    /// # Example
    /// ```rust,ignore
    /// let backend = LocalStorageBackend::new(".featureduck")?;
    /// ```
    pub fn new(base_path: impl AsRef<Path>) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();

        std::fs::create_dir_all(&base_path).map_err(|e| {
            Error::StorageError(anyhow::anyhow!(
                "Failed to create base directory '{}': {}",
                base_path.display(),
                e
            ))
        })?;

        Ok(Self { base_path })
    }

    /// Get absolute path for a remote key
    fn resolve_path(&self, remote_key: &str) -> PathBuf {
        self.base_path.join(remote_key)
    }
}

#[async_trait]
impl StorageBackend for LocalStorageBackend {
    async fn upload_file(&self, local_path: &Path, remote_key: &str) -> Result<()> {
        let remote_path = self.resolve_path(remote_key);

        if let Some(parent) = remote_path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                Error::StorageError(anyhow::anyhow!(
                    "Failed to create directory '{}': {}",
                    parent.display(),
                    e
                ))
            })?;
        }

        let bytes_copied = fs::copy(local_path, &remote_path).await.map_err(|e| {
            Error::StorageError(anyhow::anyhow!(
                "Failed to upload file '{}' to '{}': {}",
                local_path.display(),
                remote_path.display(),
                e
            ))
        })?;

        if bytes_copied == 0 {
            return Err(Error::StorageError(anyhow::anyhow!(
                "File upload copied 0 bytes from '{}' to '{}'",
                local_path.display(),
                remote_path.display()
            )));
        }

        Ok(())
    }

    async fn download_file(&self, remote_key: &str, local_path: &Path) -> Result<()> {
        let remote_path = self.resolve_path(remote_key);

        if !remote_path.exists() {
            return Err(Error::NotFound(format!("File not found: {}", remote_key)));
        }

        if let Some(parent) = local_path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                Error::StorageError(anyhow::anyhow!(
                    "Failed to create directory '{}': {}",
                    parent.display(),
                    e
                ))
            })?;
        }

        fs::copy(&remote_path, local_path).await.map_err(|e| {
            Error::StorageError(anyhow::anyhow!(
                "Failed to download file '{}' to '{}': {}",
                remote_path.display(),
                local_path.display(),
                e
            ))
        })?;

        Ok(())
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        let prefix_path = self.resolve_path(prefix);

        if !prefix_path.exists() {
            return Ok(vec![]);
        }

        let mut objects = Vec::new();
        let mut entries = fs::read_dir(&prefix_path).await.map_err(|e| {
            Error::StorageError(anyhow::anyhow!(
                "Failed to list directory '{}': {}",
                prefix_path.display(),
                e
            ))
        })?;

        while let Some(entry) = entries.next_entry().await.map_err(|e| {
            Error::StorageError(anyhow::anyhow!("Failed to read directory entry: {}", e))
        })? {
            let path = entry.path();
            if path.is_file() {
                if let Ok(rel_path) = path.strip_prefix(&self.base_path) {
                    objects.push(rel_path.to_string_lossy().to_string());
                }
            }
        }

        Ok(objects)
    }

    async fn delete_object(&self, remote_key: &str) -> Result<()> {
        let remote_path = self.resolve_path(remote_key);

        if remote_path.exists() {
            fs::remove_file(&remote_path).await.map_err(|e| {
                Error::StorageError(anyhow::anyhow!(
                    "Failed to delete file '{}': {}",
                    remote_path.display(),
                    e
                ))
            })?;
        }

        Ok(())
    }

    async fn exists(&self, remote_key: &str) -> Result<bool> {
        let remote_path = self.resolve_path(remote_key);
        Ok(remote_path.exists())
    }

    async fn metadata(&self, remote_key: &str) -> Result<ObjectMetadata> {
        let remote_path = self.resolve_path(remote_key);

        let metadata = fs::metadata(&remote_path)
            .await
            .map_err(|e| Error::NotFound(format!("File not found '{}': {}", remote_key, e)))?;

        let modified = metadata.modified().map_err(|e| {
            Error::StorageError(anyhow::anyhow!(
                "Failed to get modified time for '{}': {}",
                remote_key,
                e
            ))
        })?;

        Ok(ObjectMetadata {
            size_bytes: metadata.len(),
            last_modified: chrono::DateTime::from(modified),
            etag: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn test_local_backend_upload_download() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalStorageBackend::new(temp_dir.path()).unwrap();

        let test_file = temp_dir.path().join("test.txt");
        let mut file = tokio::fs::File::create(&test_file).await.unwrap();
        file.write_all(b"hello world").await.unwrap();

        backend
            .upload_file(&test_file, "test/uploaded.txt")
            .await
            .unwrap();

        let download_path = temp_dir.path().join("downloaded.txt");
        backend
            .download_file("test/uploaded.txt", &download_path)
            .await
            .unwrap();

        let content = tokio::fs::read_to_string(&download_path).await.unwrap();
        assert_eq!(content, "hello world");
    }

    #[tokio::test]
    async fn test_local_backend_list_objects() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalStorageBackend::new(temp_dir.path()).unwrap();

        let test_file = temp_dir.path().join("test1.txt");
        tokio::fs::write(&test_file, b"test1").await.unwrap();
        backend
            .upload_file(&test_file, "data/file1.txt")
            .await
            .unwrap();

        let test_file2 = temp_dir.path().join("test2.txt");
        tokio::fs::write(&test_file2, b"test2").await.unwrap();
        backend
            .upload_file(&test_file2, "data/file2.txt")
            .await
            .unwrap();

        let objects = backend.list_objects("data/").await.unwrap();
        assert_eq!(objects.len(), 2);
        assert!(objects.contains(&"data/file1.txt".to_string()));
        assert!(objects.contains(&"data/file2.txt".to_string()));
    }

    #[tokio::test]
    async fn test_local_backend_exists() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalStorageBackend::new(temp_dir.path()).unwrap();

        let test_file = temp_dir.path().join("test.txt");
        tokio::fs::write(&test_file, b"test").await.unwrap();
        backend.upload_file(&test_file, "exists.txt").await.unwrap();

        assert!(backend.exists("exists.txt").await.unwrap());
        assert!(!backend.exists("not_exists.txt").await.unwrap());
    }

    #[tokio::test]
    async fn test_local_backend_delete() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalStorageBackend::new(temp_dir.path()).unwrap();

        let test_file = temp_dir.path().join("test.txt");
        tokio::fs::write(&test_file, b"test").await.unwrap();
        backend
            .upload_file(&test_file, "delete_me.txt")
            .await
            .unwrap();

        assert!(backend.exists("delete_me.txt").await.unwrap());

        backend.delete_object("delete_me.txt").await.unwrap();

        assert!(!backend.exists("delete_me.txt").await.unwrap());
    }

    #[tokio::test]
    async fn test_local_backend_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalStorageBackend::new(temp_dir.path()).unwrap();

        let test_file = temp_dir.path().join("test.txt");
        tokio::fs::write(&test_file, b"hello").await.unwrap();
        backend.upload_file(&test_file, "meta.txt").await.unwrap();

        let metadata = backend.metadata("meta.txt").await.unwrap();
        assert_eq!(metadata.size_bytes, 5);
        assert!(metadata.etag.is_none());
    }

    #[tokio::test]
    async fn test_local_backend_download_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalStorageBackend::new(temp_dir.path()).unwrap();

        let download_path = temp_dir.path().join("downloaded.txt");
        let result = backend
            .download_file("nonexistent.txt", &download_path)
            .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NotFound(_)));
    }

    #[tokio::test]
    async fn test_local_backend_metadata_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalStorageBackend::new(temp_dir.path()).unwrap();

        let result = backend.metadata("nonexistent.txt").await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NotFound(_)));
    }

    #[tokio::test]
    async fn test_local_backend_delete_idempotent() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalStorageBackend::new(temp_dir.path()).unwrap();

        backend.delete_object("never_existed.txt").await.unwrap();
        backend.delete_object("never_existed.txt").await.unwrap();
    }

    #[tokio::test]
    async fn test_local_backend_nested_directories() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalStorageBackend::new(temp_dir.path()).unwrap();

        let test_file = temp_dir.path().join("test.txt");
        tokio::fs::write(&test_file, b"nested").await.unwrap();

        backend
            .upload_file(&test_file, "deep/nested/path/file.txt")
            .await
            .unwrap();

        assert!(backend.exists("deep/nested/path/file.txt").await.unwrap());

        let download_path = temp_dir.path().join("downloaded.txt");
        backend
            .download_file("deep/nested/path/file.txt", &download_path)
            .await
            .unwrap();

        let content = tokio::fs::read_to_string(&download_path).await.unwrap();
        assert_eq!(content, "nested");
    }

    #[tokio::test]
    async fn test_local_backend_list_empty_prefix() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalStorageBackend::new(temp_dir.path()).unwrap();

        let objects = backend.list_objects("nonexistent/").await.unwrap();
        assert_eq!(objects.len(), 0);
    }

    #[tokio::test]
    async fn test_local_backend_overwrite_file() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalStorageBackend::new(temp_dir.path()).unwrap();

        let test_file1 = temp_dir.path().join("test1.txt");
        tokio::fs::write(&test_file1, b"version1").await.unwrap();
        backend
            .upload_file(&test_file1, "overwrite.txt")
            .await
            .unwrap();

        let test_file2 = temp_dir.path().join("test2.txt");
        tokio::fs::write(&test_file2, b"version2").await.unwrap();
        backend
            .upload_file(&test_file2, "overwrite.txt")
            .await
            .unwrap();

        let download_path = temp_dir.path().join("downloaded.txt");
        backend
            .download_file("overwrite.txt", &download_path)
            .await
            .unwrap();

        let content = tokio::fs::read_to_string(&download_path).await.unwrap();
        assert_eq!(content, "version2");
    }

    #[tokio::test]
    async fn test_local_backend_large_file() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalStorageBackend::new(temp_dir.path().join("storage")).unwrap();

        let large_content = vec![b'x'; 1024 * 1024];
        let test_file = temp_dir.path().join("source.bin");
        std::fs::write(&test_file, &large_content).unwrap();

        backend
            .upload_file(&test_file, "uploaded/large.bin")
            .await
            .unwrap();

        let download_path = temp_dir.path().join("downloaded.bin");
        backend
            .download_file("uploaded/large.bin", &download_path)
            .await
            .unwrap();

        let downloaded_content = tokio::fs::read(&download_path).await.unwrap();
        assert_eq!(downloaded_content.len(), 1024 * 1024);
        assert_eq!(downloaded_content, large_content);

        let metadata = backend.metadata("uploaded/large.bin").await.unwrap();
        assert_eq!(metadata.size_bytes, 1024 * 1024);
    }

    #[tokio::test]
    async fn test_local_backend_special_characters_in_key() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalStorageBackend::new(temp_dir.path()).unwrap();

        let test_file = temp_dir.path().join("test.txt");
        tokio::fs::write(&test_file, b"special").await.unwrap();

        backend
            .upload_file(&test_file, "path/with spaces/file-name_123.txt")
            .await
            .unwrap();

        assert!(backend
            .exists("path/with spaces/file-name_123.txt")
            .await
            .unwrap());
    }
}
