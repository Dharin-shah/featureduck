//! MinIO/S3-compatible storage backend
//!
//! This backend will support S3-compatible object storage (MinIO, AWS S3, etc.).
//! Currently stubbed - returns `NotImplemented` errors.
//!
//! ## Future Implementation
//!
//! When implementing MinIO support (Phase 2):
//! 1. Add `aws-sdk-s3` dependency
//! 2. Implement S3 client configuration
//! 3. Implement all `StorageBackend` methods using S3 APIs
//! 4. Add integration tests with MinIO container
//!
//! Estimated effort: 4-6 hours

use async_trait::async_trait;
use std::path::Path;

use super::{ObjectMetadata, StorageBackend};
use crate::{Error, Result};

/// MinIO/S3-compatible storage backend
///
/// TODO: Implement when adding HA/scalability support
/// For now, all methods return `NotImplemented` errors
pub struct MinIOStorageBackend {
    #[allow(dead_code)]
    endpoint: String,
    #[allow(dead_code)]
    bucket: String,
}

impl MinIOStorageBackend {
    /// Create new MinIO storage backend
    ///
    /// # Arguments
    /// * `endpoint` - MinIO endpoint URL (e.g., "http://minio:9000")
    /// * `bucket` - Bucket name
    ///
    /// # Note
    /// Currently returns a stub that will error on use.
    /// Real implementation coming in Phase 2.
    pub fn new(endpoint: String, bucket: String) -> Result<Self> {
        Ok(Self { endpoint, bucket })
    }
}

#[async_trait]
impl StorageBackend for MinIOStorageBackend {
    async fn upload_file(&self, _local_path: &Path, _remote_key: &str) -> Result<()> {
        Err(Error::NotImplemented(
            "MinIO support coming in Phase 2. Use LocalStorageBackend for now.".into(),
        ))
    }

    async fn download_file(&self, _remote_key: &str, _local_path: &Path) -> Result<()> {
        Err(Error::NotImplemented(
            "MinIO support coming in Phase 2. Use LocalStorageBackend for now.".into(),
        ))
    }

    async fn list_objects(&self, _prefix: &str) -> Result<Vec<String>> {
        Err(Error::NotImplemented(
            "MinIO support coming in Phase 2. Use LocalStorageBackend for now.".into(),
        ))
    }

    async fn delete_object(&self, _remote_key: &str) -> Result<()> {
        Err(Error::NotImplemented(
            "MinIO support coming in Phase 2. Use LocalStorageBackend for now.".into(),
        ))
    }

    async fn exists(&self, _remote_key: &str) -> Result<bool> {
        Err(Error::NotImplemented(
            "MinIO support coming in Phase 2. Use LocalStorageBackend for now.".into(),
        ))
    }

    async fn metadata(&self, _remote_key: &str) -> Result<ObjectMetadata> {
        Err(Error::NotImplemented(
            "MinIO support coming in Phase 2. Use LocalStorageBackend for now.".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_minio_backend_not_implemented() {
        let backend = MinIOStorageBackend::new("http://minio:9000".into(), "test".into()).unwrap();

        let result = backend.upload_file(Path::new("test.txt"), "key").await;
        assert!(matches!(result, Err(Error::NotImplemented(_))));

        let result = backend.download_file("key", Path::new("test.txt")).await;
        assert!(matches!(result, Err(Error::NotImplemented(_))));

        let result = backend.list_objects("prefix").await;
        assert!(matches!(result, Err(Error::NotImplemented(_))));
    }
}
