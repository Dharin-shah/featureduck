//! FeatureDuck Online Store - Blazingly Fast Feature Serving
//!
//! This crate provides low-latency online feature stores for real-time inference.
//! Features are synced from Delta Lake (offline store) to Redis or PostgreSQL (online stores).
//!
//! ## Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────────┐
//! │                      Online Feature Serving                       │
//! ├──────────────────────────────────────────────────────────────────┤
//! │                                                                   │
//! │   ┌─────────────┐      sync_to_online()     ┌───────────────┐   │
//! │   │ Delta Lake  │ ───────────────────────► │ Redis/Postgres│   │
//! │   │ (Offline)   │    100K+ rows/sec         │ (Online)      │   │
//! │   └─────────────┘                           └───────────────┘   │
//! │                                                     │            │
//! │                                                     │ <1ms P99   │
//! │                                                     ▼            │
//! │                                              ┌───────────┐       │
//! │                                              │ ML Model  │       │
//! │                                              │ Inference │       │
//! │                                              └───────────┘       │
//! │                                                                   │
//! └──────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Quick Start
//!
//! ### Redis
//!
//! ```rust,ignore
//! use featureduck_online::{RedisOnlineStore, RedisConfig, sync_to_online, SyncConfig};
//!
//! // Connect to Redis
//! let config = RedisConfig::new("redis://localhost:6379");
//! let store = RedisOnlineStore::new(config).await?;
//!
//! // Sync from Delta Lake
//! let result = sync_to_online(
//!     &store,
//!     "s3://bucket/features/user_features",
//!     "user_features",
//!     &["user_id"],
//!     SyncConfig::default()
//! ).await?;
//!
//! println!("Synced {} rows at {:.0}/sec", result.rows_synced, result.rows_per_sec);
//!
//! // Serve features (< 1ms!)
//! let features = store.get_online_features(
//!     "user_features",
//!     vec![EntityKey::new("user_id", "123")]
//! ).await?;
//! ```
//!
//! ### PostgreSQL
//!
//! ```rust,ignore
//! use featureduck_online::{PostgresOnlineStore, PostgresConfig};
//!
//! let config = PostgresConfig::new("postgresql://localhost/featureduck");
//! let store = PostgresOnlineStore::new(config).await?;
//!
//! // Same sync and serving API as Redis
//! ```
//!
//! ## Performance Targets
//!
//! | Metric | Redis | PostgreSQL |
//! |--------|-------|------------|
//! | Read latency (P99) | < 1ms | < 5ms |
//! | Write throughput | 100K/sec | 50K/sec |
//! | Sync latency (1K rows) | < 10ms | < 50ms |
//!
//! ## Feature Flags
//!
//! - `redis` (default): Enable Redis online store
//! - `postgres`: Enable PostgreSQL online store
//! - `all`: Enable all online stores

// Re-export core types
pub use featureduck_core::{
    build_online_key, parse_online_key, EntityKey, FeatureRow, FeatureValue, OnlineStore,
    OnlineWriteConfig,
};

// Sync module
pub mod sync;
pub use sync::{sync_to_online, SyncConfig, SyncResult};

// Redis store (default feature)
#[cfg(feature = "redis")]
pub mod redis_store;
#[cfg(feature = "redis")]
pub use redis_store::{RedisBulkWriteMode, RedisConfig, RedisOnlineStore};

// PostgreSQL store (optional feature)
#[cfg(feature = "postgres")]
pub mod postgres_store;
#[cfg(feature = "postgres")]
pub use postgres_store::{BulkWriteMode, PostgresConfig, PostgresOnlineStore};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_re_exports() {
        // Verify core types are re-exported
        let key = EntityKey::new("user_id", "123");
        assert_eq!(key.name, "user_id");

        let online_key = build_online_key("test", std::slice::from_ref(&key));
        assert!(online_key.contains("user_id=123"));
    }
}
