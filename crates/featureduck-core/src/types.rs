//! Core data types for FeatureDuck
//!
//! This module defines the fundamental data structures used throughout the system.
//! These types are kept simple and focused on their single responsibility.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a single entity identifier (e.g., user_id, product_id)
///
/// Features in a feature store are always associated with entities. For example:
/// - User features are keyed by user_id
/// - Product features are keyed by product_id
///
/// An EntityKey is a name-value pair that identifies a specific entity instance.
///
/// # Examples
///
/// ```
/// use featureduck_core::EntityKey;
///
/// // Create an entity key for a user
/// let user_key = EntityKey {
///     name: "user_id".to_string(),
///     value: "123".to_string(),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct EntityKey {
    /// The name of the entity column (e.g., "user_id")
    pub name: String,
    
    /// The value of the entity (e.g., "123")
    /// Stored as String for simplicity - parsing to appropriate type happens later
    pub value: String,
}

impl EntityKey {
    /// Creates a new EntityKey
    ///
    /// # Arguments
    ///
    /// * `name` - The entity column name
    /// * `value` - The entity value (will be converted to String)
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

/// Represents a feature value that can be of different types
///
/// Features can have various data types (integers, floats, strings, etc.).
/// This enum allows us to represent all possible feature value types in a
/// type-safe way.
///
/// Note: We keep this simple for MVP. Advanced types (arrays, structs) can be
/// added later if needed (YAGNI principle).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]  // Serialize without type tags for cleaner JSON
pub enum FeatureValue {
    /// Integer value (e.g., click counts, age)
    Int(i64),
    
    /// Floating point value (e.g., conversion rate, score)
    Float(f64),
    
    /// String value (e.g., category, status)
    String(String),
    
    /// Boolean value (e.g., is_premium, has_purchased)
    Bool(bool),
    
    /// Null/missing value
    Null,
}

/// Metadata about a feature view
///
/// A feature view is a collection of related features that share the same entity
/// and are stored together. For example, "user_features" might contain clicks_7d,
/// purchases_30d, etc.
///
/// This struct contains the metadata needed to query and validate features.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureView {
    /// Unique name of the feature view (e.g., "user_features")
    pub name: String,
    
    /// Names of entity columns (e.g., ["user_id"])
    /// Most feature views have a single entity, but we support composite keys
    pub entity_columns: Vec<String>,
    
    /// Names of feature columns (e.g., ["clicks_7d", "purchases_30d"])
    pub feature_columns: Vec<String>,
    
    /// Name of the timestamp column for point-in-time queries
    /// This is optional - some feature views might not have time-travel support
    pub timestamp_column: Option<String>,
    
    /// Path to the underlying data storage (e.g., "s3://bucket/features/user_features")
    pub source_path: String,
    
    /// When this feature view was created
    pub created_at: DateTime<Utc>,
    
    /// When this feature view was last updated
    pub updated_at: DateTime<Utc>,
}

impl FeatureView {
    /// Creates a new FeatureView with minimal required fields
    ///
    /// This is a convenience constructor for testing and simple use cases.
    /// For production use, you'd typically load this from the registry.
    pub fn new(
        name: impl Into<String>,
        entity_columns: Vec<String>,
        feature_columns: Vec<String>,
        source_path: impl Into<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            name: name.into(),
            entity_columns,
            feature_columns,
            timestamp_column: None,
            source_path: source_path.into(),
            created_at: now,
            updated_at: now,
        }
    }
}

/// Represents a row of features for a single entity
///
/// This is the response type when fetching features. Each row contains:
/// - The entity key(s) that identify the row
/// - The feature values for that entity
/// - Optional timestamp (for historical queries)
///
/// # Examples
///
/// ```
/// use featureduck_core::{FeatureRow, EntityKey, FeatureValue};
/// use std::collections::HashMap;
///
/// let mut features = HashMap::new();
/// features.insert("clicks_7d".to_string(), FeatureValue::Int(42));
/// features.insert("purchases_30d".to_string(), FeatureValue::Int(3));
///
/// let row = FeatureRow {
///     entities: vec![EntityKey::new("user_id", "123")],
///     features,
///     timestamp: None,
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureRow {
    /// Entity identifiers for this row
    pub entities: Vec<EntityKey>,
    
    /// Feature name -> value mapping
    pub features: HashMap<String, FeatureValue>,
    
    /// Timestamp for this feature snapshot (optional)
    /// Used for point-in-time queries to indicate when features were valid
    pub timestamp: Option<DateTime<Utc>>,
}

impl FeatureRow {
    /// Creates a new empty FeatureRow for the given entities
    pub fn new(entities: Vec<EntityKey>) -> Self {
        Self {
            entities,
            features: HashMap::new(),
            timestamp: None,
        }
    }
    
    /// Adds a feature to this row
    pub fn with_feature(mut self, name: impl Into<String>, value: FeatureValue) -> Self {
        self.features.insert(name.into(), value);
        self
    }
    
    /// Sets the timestamp for this row
    pub fn with_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.timestamp = Some(timestamp);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entity_key_creation() {
        let key = EntityKey::new("user_id", "123");
        assert_eq!(key.name, "user_id");
        assert_eq!(key.value, "123");
    }

    #[test]
    fn test_feature_row_builder() {
        let row = FeatureRow::new(vec![EntityKey::new("user_id", "123")])
            .with_feature("clicks_7d", FeatureValue::Int(42))
            .with_feature("is_premium", FeatureValue::Bool(true));
        
        assert_eq!(row.entities.len(), 1);
        assert_eq!(row.features.len(), 2);
        assert_eq!(row.features.get("clicks_7d"), Some(&FeatureValue::Int(42)));
    }

    #[test]
    fn test_feature_value_serialization() {
        // Test that FeatureValue serializes to clean JSON
        let value = FeatureValue::Int(42);
        let json = serde_json::to_string(&value).unwrap();
        assert_eq!(json, "42");  // Not {"Int": 42} because of #[serde(untagged)]
    }
}
