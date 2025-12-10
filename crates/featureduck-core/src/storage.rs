//! Storage backend abstraction for incremental state
//!
//! This module provides an abstraction layer for storing incremental state files.
//! The abstraction allows switching between different storage backends (local filesystem,
//! MinIO/S3, Azure Blob, GCS) without changing business logic.
//!
//! ## Design Principle
//!
//! The `StorageBackend` trait defines the interface. Business logic uses only the trait,
//! never concrete implementations. This allows:
//! - Start simple with local filesystem (works now)
//! - Add cloud storage later (MinIO, S3, etc.)
//! - Switch backends via configuration (no code changes)
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use featureduck_core::storage::{StorageBackend, LocalStorageBackend};
//!
//! let backend = LocalStorageBackend::new(".featureduck")?;
//! backend.upload_file(Path::new("state.duckdb"), "incremental/user_features/state_v1.duckdb").await?;
//! ```

use async_trait::async_trait;
use std::path::Path;

use crate::Result;

/// Object metadata returned by storage backends
#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    /// Size in bytes
    pub size_bytes: u64,

    /// Last modification timestamp
    pub last_modified: chrono::DateTime<chrono::Utc>,

    /// ETag for versioning (S3/MinIO only)
    pub etag: Option<String>,
}

/// Abstract storage backend for incremental state
///
/// This trait defines the interface for uploading, downloading, and listing objects
/// in a storage backend. Implementations exist for:
/// - `LocalStorageBackend`: Local filesystem (native, works now)
/// - `MinIOStorageBackend`: S3-compatible object storage (future)
///
/// ## Future Implementations
/// - `AzureBlobBackend`: Azure Blob Storage
/// - `GCSBackend`: Google Cloud Storage
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Upload file to storage
    ///
    /// # Arguments
    /// * `local_path` - Path to local file to upload
    /// * `remote_key` - Remote key (e.g., "incremental/user_features/state_v1.duckdb")
    ///
    /// # Example
    /// ```rust,ignore
    /// backend.upload_file(Path::new("state.duckdb"), "incremental/user_features/state_v1.duckdb").await?;
    /// ```
    async fn upload_file(&self, local_path: &Path, remote_key: &str) -> Result<()>;

    /// Download file from storage
    ///
    /// # Arguments
    /// * `remote_key` - Remote key
    /// * `local_path` - Where to save locally
    ///
    /// # Errors
    /// Returns `Error::NotFound` if remote key doesn't exist
    async fn download_file(&self, remote_key: &str, local_path: &Path) -> Result<()>;

    /// List objects with prefix
    ///
    /// # Arguments
    /// * `prefix` - Key prefix (e.g., "incremental/user_features/")
    ///
    /// # Returns
    /// List of keys matching prefix (relative to base path)
    ///
    /// # Example
    /// ```rust,ignore
    /// let objects = backend.list_objects("incremental/user_features/").await?;
    /// // Returns: ["incremental/user_features/state_v1.duckdb", "incremental/user_features/state_v2.duckdb"]
    /// ```
    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>>;

    /// Delete object
    ///
    /// # Arguments
    /// * `remote_key` - Key to delete
    ///
    /// # Note
    /// This is idempotent - deleting a non-existent key succeeds
    async fn delete_object(&self, remote_key: &str) -> Result<()>;

    /// Check if object exists
    ///
    /// # Arguments
    /// * `remote_key` - Key to check
    ///
    /// # Returns
    /// `true` if object exists, `false` otherwise
    async fn exists(&self, remote_key: &str) -> Result<bool>;

    /// Get object metadata
    ///
    /// # Arguments
    /// * `remote_key` - Key to get metadata for
    ///
    /// # Returns
    /// Metadata (size, last modified, etag)
    ///
    /// # Errors
    /// Returns `Error::NotFound` if remote key doesn't exist
    async fn metadata(&self, remote_key: &str) -> Result<ObjectMetadata>;
}

pub mod local;
pub mod minio;
