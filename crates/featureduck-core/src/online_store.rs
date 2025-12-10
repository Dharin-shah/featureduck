//! Online store trait for low-latency feature serving
//!
//! Online stores provide sub-10ms P99 latency feature retrieval for real-time
//! inference. Data is synced from the offline store (Delta Lake) to online stores
//! (Redis, PostgreSQL) for fast access.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────┐      Sync       ┌────────────────────┐
//! │  Offline Store      │ ──────────────► │   Online Store     │
//! │  (Delta Lake)       │                 │  (Redis/Postgres)  │
//! │  - Batch features   │                 │  - Latest values   │
//! │  - Historical data  │                 │  - Low latency     │
//! │  - ACID writes      │                 │  - Key-value access│
//! └─────────────────────┘                 └────────────────────┘
//! ```
//!
//! ## Key Design Decisions
//!
//! 1. **Simple key-value model**: Entity keys → feature values
//! 2. **Only latest values**: No point-in-time queries (use offline store for that)
//! 3. **Batch operations**: Get/set multiple entities efficiently
//! 4. **TTL support**: Features can expire automatically
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use featureduck_core::{OnlineStore, EntityKey, FeatureRow};
//!
//! async fn serve_features(store: &dyn OnlineStore) {
//!     let entities = vec![EntityKey::new("user_id", "123")];
//!     let features = store.get_online_features("user_features", entities).await?;
//!     // P99 < 10ms
//! }
//! ```

use crate::{EntityKey, FeatureRow, Result};
use async_trait::async_trait;
use std::time::Duration;

/// Configuration for online store write operations
#[derive(Debug, Clone, Default)]
pub struct OnlineWriteConfig {
    /// Time-to-live for features (None = no expiration)
    pub ttl: Option<Duration>,
}

impl OnlineWriteConfig {
    /// Create config with TTL
    pub fn with_ttl(ttl: Duration) -> Self {
        Self { ttl: Some(ttl) }
    }
}

/// Trait for online feature stores (Redis, PostgreSQL, etc.)
///
/// Online stores provide low-latency access to the latest feature values.
/// They are designed for real-time feature serving during model inference.
///
/// ## Implementation Requirements
///
/// - `get_online_features`: < 10ms P99 latency for 100 entities
/// - `write_online_features`: Batch writes for efficient sync
/// - Thread-safe (Send + Sync)
///
/// ## Key Format
///
/// The default key format is: `{feature_view}:{entity_key_name}:{entity_key_value}`
/// For composite keys: `{feature_view}:{key1_name}={key1_value}:{key2_name}={key2_value}`
///
/// Implementations may override this for custom key formats.
#[async_trait]
pub trait OnlineStore: Send + Sync {
    /// Get features for the specified entities (latest values only)
    ///
    /// This is the primary method for online serving. It returns the latest
    /// feature values for the given entities.
    ///
    /// # Arguments
    ///
    /// * `feature_view` - Name of the feature view
    /// * `entity_keys` - Entity identifiers to fetch
    ///
    /// # Returns
    ///
    /// Vector of FeatureRow, one per entity. Missing entities return empty features.
    ///
    /// # Performance Target
    ///
    /// P99 latency < 10ms for batch of 100 entities
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let entities = vec![
    ///     EntityKey::new("user_id", "123"),
    ///     EntityKey::new("user_id", "456"),
    /// ];
    /// let features = store.get_online_features("user_features", entities).await?;
    /// ```
    async fn get_online_features(
        &self,
        feature_view: &str,
        entity_keys: Vec<EntityKey>,
    ) -> Result<Vec<FeatureRow>>;

    /// Write features to the online store (upsert semantics)
    ///
    /// This overwrites existing features for the given entities.
    /// Used during sync from offline store to online store.
    ///
    /// # Arguments
    ///
    /// * `feature_view` - Name of the feature view
    /// * `rows` - Feature rows to write
    /// * `config` - Write configuration (TTL, etc.)
    ///
    /// # Semantics
    ///
    /// - Upsert: Insert if not exists, update if exists
    /// - Atomic per entity (all features for an entity are written together)
    /// - May use pipelining/batching for efficiency
    async fn write_online_features(
        &self,
        feature_view: &str,
        rows: Vec<FeatureRow>,
        config: OnlineWriteConfig,
    ) -> Result<()>;

    /// Delete features for specified entities
    ///
    /// Used for GDPR compliance or data cleanup.
    async fn delete_online_features(
        &self,
        feature_view: &str,
        entity_keys: Vec<EntityKey>,
    ) -> Result<()>;

    /// Check if the online store is healthy and ready
    ///
    /// Returns Ok(()) if the store is ready to serve requests.
    async fn health_check(&self) -> Result<()>;

    /// Get the name of this online store type (for logging/metrics)
    fn store_type(&self) -> &'static str;
}

/// Build a composite key from entity keys
///
/// Format: `{feature_view}:{key1_name}={key1_value}:{key2_name}={key2_value}`
///
/// This is the default key format used by online stores.
/// Keys are sorted by name for consistency.
pub fn build_online_key(feature_view: &str, entity_keys: &[EntityKey]) -> String {
    let mut parts = Vec::with_capacity(entity_keys.len() + 1);
    parts.push(feature_view.to_string());

    // Sort by entity name for consistent key ordering
    let mut sorted_keys: Vec<_> = entity_keys.iter().collect();
    sorted_keys.sort_by_key(|k| &k.name);

    for key in sorted_keys {
        parts.push(format!("{}={}", key.name, key.value));
    }

    parts.join(":")
}

/// Parse entity keys from a composite key
///
/// Reverse of `build_online_key`. Returns (feature_view, entity_keys).
pub fn parse_online_key(key: &str) -> Option<(String, Vec<EntityKey>)> {
    let parts: Vec<&str> = key.split(':').collect();
    if parts.is_empty() {
        return None;
    }

    let feature_view = parts[0].to_string();
    let mut entity_keys = Vec::with_capacity(parts.len() - 1);

    for part in &parts[1..] {
        let kv: Vec<&str> = part.splitn(2, '=').collect();
        if kv.len() == 2 {
            entity_keys.push(EntityKey::new(kv[0], kv[1]));
        }
    }

    Some((feature_view, entity_keys))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_online_key_single_entity() {
        let key = build_online_key(
            "user_features",
            &[EntityKey::new("user_id", "123")],
        );
        assert_eq!(key, "user_features:user_id=123");
    }

    #[test]
    fn test_build_online_key_composite() {
        let key = build_online_key(
            "user_product_features",
            &[
                EntityKey::new("product_id", "456"),
                EntityKey::new("user_id", "123"),
            ],
        );
        // Keys are sorted alphabetically by name
        assert_eq!(key, "user_product_features:product_id=456:user_id=123");
    }

    #[test]
    fn test_parse_online_key() {
        let (view, keys) = parse_online_key("user_features:user_id=123").unwrap();
        assert_eq!(view, "user_features");
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].name, "user_id");
        assert_eq!(keys[0].value, "123");
    }

    #[test]
    fn test_parse_online_key_composite() {
        let (view, keys) = parse_online_key("user_product_features:product_id=456:user_id=123").unwrap();
        assert_eq!(view, "user_product_features");
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn test_online_write_config_default() {
        let config = OnlineWriteConfig::default();
        assert!(config.ttl.is_none());
    }

    #[test]
    fn test_online_write_config_with_ttl() {
        let config = OnlineWriteConfig::with_ttl(Duration::from_secs(3600));
        assert_eq!(config.ttl, Some(Duration::from_secs(3600)));
    }
}
