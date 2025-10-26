//! # Delta Lake Storage Connector
//!
//! This crate provides a Delta Lake implementation of the StorageConnector trait.
//!
//! ## Status: Milestone 1 (To Be Implemented)
//!
//! This is a placeholder for Milestone 1. We'll implement the DeltaStorageConnector
//! using TDD (Test-Driven Development).
//!
//! ## What We'll Build
//!
//! ```rust,ignore
//! use featureduck_core::{StorageConnector, EntityKey};
//! use featureduck_delta::DeltaStorageConnector;
//!
//! // Create connector
//! let connector = DeltaStorageConnector::new("s3://bucket/features").await?;
//!
//! // Read features
//! let entities = vec![EntityKey::new("user_id", "123")];
//! let features = connector.read_features("user_features", entities, None).await?;
//! ```
//!
//! ## Key Features (Milestone 1)
//!
//! - Read from Delta Lake tables (local or S3)
//! - Point-in-time queries using Delta Lake time travel
//! - Write features to Delta Lake
//! - List available feature views
//!
//! ## Implementation Strategy (TDD)
//!
//! 1. Write test for reading features (Red)
//! 2. Implement minimal code to pass (Green)
//! 3. Refactor for quality (Refactor)
//! 4. Repeat for write, list, etc.

// Re-export key types that users of this crate will need
pub use connector::DeltaStorageConnector;

// Module declarations
mod connector;

// This will be implemented in Milestone 1
