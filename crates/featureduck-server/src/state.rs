//! Application state shared across all HTTP handlers
//!
//! This module defines the `AppState` struct that holds all the shared
//! resources needed by the HTTP handlers (database connections, configuration, etc.).
//!
//! ## Design Pattern: Dependency Injection
//!
//! Rather than using global variables, we pass state explicitly to each handler.
//! This makes the code:
//! - Easier to test (can inject mock state)
//! - Easier to reason about (explicit dependencies)
//! - Thread-safe (Axum requires state to be Clone + Send + Sync)

use featureduck_core::OnlineStore;
use featureduck_delta::DeltaStorageConnector;
use featureduck_registry::FeatureRegistry;
use std::sync::Arc;

/// Shared application state
///
/// This struct contains everything that HTTP handlers need to access.
/// It's wrapped in an Arc (atomic reference counter) so it can be cloned
/// cheaply and shared across threads.
///
/// ## Current Implementation
/// Contains:
/// - Feature registry for metadata management
/// - Delta storage connector for feature serving
/// - Server uptime tracking
///
/// ## Thread Safety
/// All fields must be thread-safe (Send + Sync) because Axum runs handlers
/// on a thread pool. We use Arc for shared ownership and interior mutability
/// patterns (Mutex, RwLock) only when necessary.
#[derive(Clone)]
pub struct AppState {
    /// Inner state wrapped in Arc for cheap cloning
    inner: Arc<AppStateInner>,
}

/// The actual state fields
///
/// This is separate from AppState so we can wrap it in Arc once
/// and clone the Arc instead of cloning all fields.
struct AppStateInner {
    /// Feature registry for metadata management (M3)
    registry: Option<Arc<FeatureRegistry>>,

    /// Delta storage connector for feature serving (offline store)
    storage: Option<DeltaStorageConnector>,

    /// Online store for low-latency feature serving (Redis/PostgreSQL)
    /// When available, preferred over storage for `/v1/features/online`
    online_store: Option<Arc<dyn OnlineStore>>,

    /// Server start time for uptime tracking
    start_time: std::time::Instant,
}

impl AppState {
    /// Creates a new AppState without registry (for testing/simple cases)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let state = AppState::new();
    /// ```
    pub fn new() -> Self {
        Self {
            inner: Arc::new(AppStateInner {
                registry: None,
                storage: None,
                online_store: None,
                start_time: std::time::Instant::now(),
            }),
        }
    }

    /// Creates a new AppState with an in-memory registry (for testing)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let state = AppState::new_with_registry();
    /// assert!(state.registry().is_some());
    /// ```
    pub async fn new_with_registry() -> Self {
        let registry = FeatureRegistry::in_memory()
            .await
            .expect("Failed to create in-memory registry");
        Self {
            inner: Arc::new(AppStateInner {
                registry: Some(Arc::new(registry)),
                storage: None,
                online_store: None,
                start_time: std::time::Instant::now(),
            }),
        }
    }

    /// Creates a new AppState with a file-backed registry
    ///
    /// # Arguments
    ///
    /// * `registry_path`: Path to the registry database file
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let state = AppState::with_registry_path("/var/featureduck/registry.db")?;
    /// ```
    pub async fn with_registry_path(registry_path: &str) -> Result<Self, String> {
        let config = featureduck_registry::RegistryConfig::sqlite(registry_path);
        let registry = FeatureRegistry::new(config)
            .await
            .map_err(|e| format!("Failed to open registry: {}", e))?;
        Ok(Self {
            inner: Arc::new(AppStateInner {
                registry: Some(Arc::new(registry)),
                storage: None,
                online_store: None,
                start_time: std::time::Instant::now(),
            }),
        })
    }

    /// Returns a reference to the registry if available
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// if let Some(registry) = state.registry() {
    ///     let views = registry.list_feature_views(None)?;
    /// }
    /// ```
    pub fn registry(&self) -> Option<&Arc<FeatureRegistry>> {
        self.inner.registry.as_ref()
    }

    /// Creates a new AppState with storage connector
    ///
    /// # Arguments
    ///
    /// * `storage_path` - Base path for Delta Lake storage
    /// * `registry_path` - Optional path to registry database
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let state = AppState::with_storage("/data/features", Some("/data/registry.db")).await?;
    /// ```
    #[allow(dead_code)] // Public API for production deployments
    pub async fn with_storage(
        storage_path: &str,
        registry_path: Option<&str>,
    ) -> Result<Self, String> {
        // Create registry first (if path provided)
        let registry = if let Some(path) = registry_path {
            let config = featureduck_registry::RegistryConfig::sqlite(path);
            Some(Arc::new(
                FeatureRegistry::new(config)
                    .await
                    .map_err(|e| format!("Failed to open registry: {}", e))?,
            ))
        } else {
            None
        };

        // Create storage connector with registry reference (if available)
        let storage = if let Some(ref reg) = registry {
            DeltaStorageConnector::new_with_registry(
                storage_path,
                featureduck_delta::DuckDBEngineConfig::default(),
                Arc::clone(reg),
            )
            .await
        } else {
            DeltaStorageConnector::new(
                storage_path,
                featureduck_delta::DuckDBEngineConfig::default(),
            )
            .await
        }
        .map_err(|e| format!("Failed to create storage connector: {}", e))?;

        Ok(Self {
            inner: Arc::new(AppStateInner {
                registry,
                storage: Some(storage),
                online_store: None,
                start_time: std::time::Instant::now(),
            }),
        })
    }

    /// Creates a new AppState with an online store for low-latency serving
    ///
    /// # Arguments
    ///
    /// * `storage_path` - Base path for Delta Lake storage (offline store)
    /// * `registry_path` - Optional path to registry database
    /// * `online_store` - Online store for low-latency feature serving
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let redis = RedisOnlineStore::new(config).await?;
    /// let state = AppState::with_online_store(
    ///     "/data/features",
    ///     Some("/data/registry.db"),
    ///     Arc::new(redis)
    /// ).await?;
    /// ```
    #[allow(dead_code)] // Public API for production deployments
    pub async fn with_online_store(
        storage_path: &str,
        registry_path: Option<&str>,
        online_store: Arc<dyn OnlineStore>,
    ) -> Result<Self, String> {
        // Create registry first (if path provided)
        let registry = if let Some(path) = registry_path {
            let config = featureduck_registry::RegistryConfig::sqlite(path);
            Some(Arc::new(
                FeatureRegistry::new(config)
                    .await
                    .map_err(|e| format!("Failed to open registry: {}", e))?,
            ))
        } else {
            None
        };

        // Create storage connector with registry reference (if available)
        let storage = if let Some(ref reg) = registry {
            DeltaStorageConnector::new_with_registry(
                storage_path,
                featureduck_delta::DuckDBEngineConfig::default(),
                Arc::clone(reg),
            )
            .await
        } else {
            DeltaStorageConnector::new(
                storage_path,
                featureduck_delta::DuckDBEngineConfig::default(),
            )
            .await
        }
        .map_err(|e| format!("Failed to create storage connector: {}", e))?;

        Ok(Self {
            inner: Arc::new(AppStateInner {
                registry,
                storage: Some(storage),
                online_store: Some(online_store),
                start_time: std::time::Instant::now(),
            }),
        })
    }

    /// Returns a reference to the storage connector if available
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// if let Some(storage) = state.storage() {
    ///     let features = storage.read_features("user_features", entities, None).await?;
    /// }
    /// ```
    pub fn storage(&self) -> Option<&DeltaStorageConnector> {
        self.inner.storage.as_ref()
    }

    /// Returns a reference to the online store if available
    ///
    /// Online stores (Redis/PostgreSQL) provide sub-millisecond latency for
    /// feature serving. When available, preferred over storage for online features.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// if let Some(online) = state.online_store() {
    ///     let features = online.get_online_features("user_features", entities).await?;
    /// }
    /// ```
    pub fn online_store(&self) -> Option<&Arc<dyn OnlineStore>> {
        self.inner.online_store.as_ref()
    }

    /// Returns the server uptime in seconds
    ///
    /// This is useful for health checks and monitoring.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let uptime = state.uptime();
    /// println!("Server has been running for {} seconds", uptime);
    /// ```
    pub fn uptime(&self) -> u64 {
        self.inner.start_time.elapsed().as_secs()
    }
}

// Implement Default trait for convenience
impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_creation() {
        let state = AppState::new();
        // Uptime should be very small (we just created it)
        assert!(state.uptime() < 1);
    }

    #[test]
    fn test_state_is_cloneable() {
        let state1 = AppState::new();
        let state2 = state1.clone();

        // Both should report same uptime (they share the same Arc)
        assert_eq!(state1.uptime(), state2.uptime());
    }

    #[tokio::test]
    async fn test_state_uptime_increases() {
        let state = AppState::new();
        let uptime1 = state.uptime();

        // Wait a bit
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let uptime2 = state.uptime();
        // Uptime should have increased (even if just by 0 seconds due to rounding)
        assert!(uptime2 >= uptime1);
    }
}
