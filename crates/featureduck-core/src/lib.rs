//! # FeatureDuck Core Library
//!
//! This is the foundation library for FeatureDuck, containing core types and traits
//! that are used across the entire system.
//!
//! ## Architecture Principle: KISS (Keep It Simple, Stupid)
//!
//! This crate intentionally has minimal dependencies and focuses on defining
//! clean interfaces rather than complex implementations. The goal is to make
//! it easy to:
//! - Understand the core concepts
//! - Add new storage connectors
//! - Test components in isolation
//!
//! ## Key Components
//!
//! - **StorageConnector**: The trait that all storage backends implement
//! - **Types**: Core data structures (EntityKey, FeatureView, etc.)
//! - **Errors**: Strongly-typed error handling
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use featureduck_core::{StorageConnector, EntityKey};
//!
//! async fn fetch_features(connector: &dyn StorageConnector) {
//!     let entities = vec![EntityKey::new("user_id", "123")];
//!     let features = connector.read_features("user_features", entities, None).await?;
//!     println!("Features: {:?}", features);
//! }
//! ```

// Re-export commonly used types for convenience
// This allows users to do `use featureduck_core::EntityKey` instead of
// `use featureduck_core::types::EntityKey`
pub use connector::StorageConnector;
pub use error::{Error, Result};
pub use types::{EntityKey, FeatureRow, FeatureValue, FeatureView};

// Module declarations
// Each module is a separate file in the src/ directory
mod connector;
mod error;
mod types;

// Prelude module - commonly used imports
// Users can do `use featureduck_core::prelude::*` to get everything they need
pub mod prelude {
    pub use crate::connector::StorageConnector;
    pub use crate::error::{Error, Result};
    pub use crate::types::{EntityKey, FeatureRow, FeatureValue, FeatureView};
}
