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
pub use online_store::{build_online_key, parse_online_key, OnlineStore, OnlineWriteConfig};
pub use state::StateStats;
pub use types::{
    AggExpr, AggFunction, BinaryOperator, EntityKey, Expr, FeatureRow, FeatureValue, FeatureView,
    Literal, UnaryOperator,
};
pub use validation::{FeatureSchema, TypeConstraint, ValueConstraint};

// Module declarations
// Each module is a separate file in the src/ directory
pub mod aggregate_tree;
mod connector;
mod error;
pub mod hyperloglog;
pub mod incremental;
pub mod online_store;
pub mod resource_limits;
pub mod retry;
pub mod state;
pub mod storage;
pub mod time_window;
mod types;
pub mod validation;

// Utility functions
use std::sync::{Mutex, MutexGuard};

/// Recovers from a poisoned mutex gracefully
///
/// In production systems, a thread panic while holding a mutex should not cascade
/// to kill the entire service. This function:
/// 1. Attempts to acquire the mutex normally
/// 2. If poisoned, logs an error but recovers the inner data
/// 3. Returns a guard allowing continued operation
///
/// # Arguments
/// * `mutex` - The mutex to acquire
/// * `component` - Name of the component for logging (e.g., "QueryCache", "StateManager")
///
/// # Returns
/// `Result<MutexGuard>` - Either a normal guard or a recovered guard
///
/// # Example
/// ```rust,ignore
/// use featureduck_core::recover_mutex;
/// use std::sync::Mutex;
///
/// let mutex = Mutex::new(42);
/// let guard = recover_mutex(&mutex, "MyComponent")?;
/// assert_eq!(*guard, 42);
/// ```
///
/// # Error Recovery Strategy
/// When a mutex is poisoned:
/// - **Before**: Thread B calls `.expect()` → Thread B panics → Cascading failure
/// - **After**: Thread B calls `recover_mutex()` → Logs error → Service continues (degraded but functional)
///
/// This is a critical production hardening fix (GAP #2 from FOUNDATIONAL_AUDIT.md).
pub fn recover_mutex<'a, T>(mutex: &'a Mutex<T>, component: &str) -> Result<MutexGuard<'a, T>> {
    match mutex.lock() {
        Ok(guard) => Ok(guard),
        Err(poisoned) => {
            tracing::error!(
                component = component,
                "Mutex poisoned (recovered) - a thread panicked while holding this lock. \
                 Service will continue but investigate the root cause immediately."
            );
            // Recover the mutex by taking ownership of the poisoned data
            // This is safe because we log the error and allow the service to continue
            Ok(poisoned.into_inner())
        }
    }
}

// Prelude module - commonly used imports
// Users can do `use featureduck_core::prelude::*` to get everything they need
pub mod prelude {
    pub use crate::connector::StorageConnector;
    pub use crate::error::{Error, Result};
    pub use crate::online_store::{OnlineStore, OnlineWriteConfig};
    pub use crate::types::{EntityKey, FeatureRow, FeatureValue, FeatureView};
    pub use crate::validation::{FeatureSchema, TypeConstraint, ValueConstraint};
}
