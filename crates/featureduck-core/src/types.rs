//! Core data types for FeatureDuck
//!
//! This module defines the fundamental data structures used throughout the system.
//! These types are kept simple and focused on their single responsibility.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Expression (column references, literals, operations)
///
/// Moved from logical_plan.rs to support incremental processing and other modules
/// that need expression types without depending on the full LogicalPlan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Hash)]
pub enum Expr {
    /// Column reference
    Column(String),

    /// Literal value
    Literal(Literal),

    /// Binary operation (a + b, a > b, etc.)
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },

    /// Unary operation (NOT a, -a, etc.)
    UnaryOp { op: UnaryOperator, expr: Box<Expr> },

    /// Function call
    Function { name: String, args: Vec<Expr> },

    /// CASE WHEN expression
    Case {
        conditions: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },
}

impl Expr {
    pub fn column(name: &str) -> Self {
        Self::Column(name.to_string())
    }

    pub fn literal<T: Into<Literal>>(value: T) -> Self {
        Self::Literal(value.into())
    }

    // Comparison operators
    pub fn eq(left: Expr, right: Expr) -> Self {
        Self::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Eq,
            right: Box::new(right),
        }
    }

    pub fn neq(left: Expr, right: Expr) -> Self {
        Self::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Neq,
            right: Box::new(right),
        }
    }

    pub fn gt(left: Expr, right: Expr) -> Self {
        Self::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Gt,
            right: Box::new(right),
        }
    }

    pub fn gte(left: Expr, right: Expr) -> Self {
        Self::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Gte,
            right: Box::new(right),
        }
    }

    pub fn lt(left: Expr, right: Expr) -> Self {
        Self::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Lt,
            right: Box::new(right),
        }
    }

    pub fn lte(left: Expr, right: Expr) -> Self {
        Self::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Lte,
            right: Box::new(right),
        }
    }

    // Logical operators
    pub fn and(left: Expr, right: Expr) -> Self {
        Self::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::And,
            right: Box::new(right),
        }
    }

    pub fn or(left: Expr, right: Expr) -> Self {
        Self::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Or,
            right: Box::new(right),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn not(expr: Expr) -> Self {
        Self::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(expr),
        }
    }

    // Arithmetic operators
    #[allow(clippy::should_implement_trait)]
    pub fn add(left: Expr, right: Expr) -> Self {
        Self::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Add,
            right: Box::new(right),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn sub(left: Expr, right: Expr) -> Self {
        Self::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Sub,
            right: Box::new(right),
        }
    }

    // Time functions
    pub fn current_timestamp() -> Self {
        Self::Function {
            name: "current_timestamp".to_string(),
            args: vec![],
        }
    }

    pub fn days_ago(days: i32) -> Self {
        Self::BinaryOp {
            left: Box::new(Self::current_timestamp()),
            op: BinaryOperator::Sub,
            right: Box::new(Self::Function {
                name: "interval_days".to_string(),
                args: vec![Self::Literal(Literal::Int(days as i64))],
            }),
        }
    }
}

/// Literal value
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Literal {
    Null,
    Boolean(bool),
    Int(i64),
    Float(f64),
    String(String),
}

// Manual Hash implementation for Literal to handle f64
impl std::hash::Hash for Literal {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Literal::Null => 0.hash(state),
            Literal::Boolean(b) => {
                1.hash(state);
                b.hash(state);
            }
            Literal::Int(i) => {
                2.hash(state);
                i.hash(state);
            }
            Literal::Float(f) => {
                3.hash(state);
                // Convert f64 to bits for hashing
                f.to_bits().hash(state);
            }
            Literal::String(s) => {
                4.hash(state);
                s.hash(state);
            }
        }
    }
}

impl From<i64> for Literal {
    fn from(v: i64) -> Self {
        Self::Int(v)
    }
}

impl From<f64> for Literal {
    fn from(v: f64) -> Self {
        Self::Float(v)
    }
}

impl From<String> for Literal {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}

impl From<&str> for Literal {
    fn from(v: &str) -> Self {
        Self::String(v.to_string())
    }
}

impl From<bool> for Literal {
    fn from(v: bool) -> Self {
        Self::Boolean(v)
    }
}

/// Binary operators
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Hash)]
pub enum BinaryOperator {
    // Comparison
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,

    // Logical
    And,
    Or,

    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

/// Unary operators
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Hash)]
pub enum UnaryOperator {
    Not,
    Minus,
}

/// Aggregation function types for incremental processing
///
/// Defines the types of aggregations that can be incrementally merged.
/// Each function has specific merge semantics:
/// - COUNT/SUM: Additive (old + new)
/// - AVG: Derived from SUM/COUNT
/// - MIN/MAX: Idempotent (take min/max of old and new)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Hash)]
pub enum AggFunction {
    Count(Expr),
    CountAll,
    Sum(Expr),
    Avg(Expr),
    Min(Expr),
    Max(Expr),
    CountDistinct(Expr),
}

/// Aggregation expression with optional alias
///
/// Represents an aggregation operation with an optional output column name.
/// The alias is used to identify the feature in the output FeatureRow.
///
/// # Example
/// ```
/// use featureduck_core::types::{AggExpr, AggFunction, Expr};
///
/// let agg = AggExpr {
///     function: AggFunction::Sum(Expr::column("amount")),
///     alias: Some("total_spent".to_string()),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Hash)]
pub struct AggExpr {
    pub function: AggFunction,
    pub alias: Option<String>,
}

impl AggExpr {
    pub fn new(function: AggFunction) -> Self {
        Self {
            function,
            alias: None,
        }
    }

    pub fn alias(mut self, name: &str) -> Self {
        self.alias = Some(name.to_string());
        self
    }

    pub fn count(column: &str) -> Self {
        Self::new(AggFunction::Count(Expr::column(column)))
    }

    pub fn count_all() -> Self {
        Self::new(AggFunction::CountAll)
    }

    pub fn sum(column: &str) -> Self {
        Self::new(AggFunction::Sum(Expr::column(column)))
    }

    pub fn avg(column: &str) -> Self {
        Self::new(AggFunction::Avg(Expr::column(column)))
    }

    pub fn min(column: &str) -> Self {
        Self::new(AggFunction::Min(Expr::column(column)))
    }

    pub fn max(column: &str) -> Self {
        Self::new(AggFunction::Max(Expr::column(column)))
    }
}

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
/// ## Type Support
///
/// **Basic Types (Production-ready):**
/// - `Int(i64)` - Integer values (click counts, age, counts)
/// - `Float(f64)` - Floating point values (rates, scores, averages)
/// - `String(String)` - Text values (categories, status, labels)
/// - `Bool(bool)` - Boolean flags (is_premium, has_purchased)
/// - `Null` - Missing or undefined values
///
/// **Advanced Types (Extended support):**
/// - `Json(serde_json::Value)` - Nested JSON data (user preferences, metadata)
/// - `ArrayInt(Vec<i64>)` - Integer arrays (item IDs, category IDs)
/// - `ArrayFloat(Vec<f64>)` - Float arrays (embeddings, scores)
/// - `ArrayString(Vec<String>)` - String arrays (tags, categories)
/// - `Date(chrono::NaiveDate)` - Date-only values (birth_date, signup_date)
///
/// ## Serialization
///
/// Uses `#[serde(untagged)]` for cleaner JSON representation:
/// - `Int(42)` → `42`
/// - `String("test")` → `"test"`
/// - `Json({...})` → `{...}` (nested object)
/// - `ArrayInt([1,2,3])` → `[1,2,3]`
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)] // Serialize without type tags for cleaner JSON
pub enum FeatureValue {
    /// Null/missing value
    /// IMPORTANT: Must be first for untagged deserialization to work correctly
    /// (otherwise Json(serde_json::Value::Null) matches before Null)
    Null,

    /// Integer value (e.g., click counts, age)
    Int(i64),

    /// Floating point value (e.g., conversion rate, score)
    Float(f64),

    /// String value (e.g., category, status)
    String(String),

    /// Boolean value (e.g., is_premium, has_purchased)
    Bool(bool),

    /// Array of integers (e.g., item IDs, category IDs)
    ///
    /// Use this for:
    /// - List of IDs (purchased_item_ids, clicked_product_ids)
    /// - Category memberships
    /// - Rankings
    ArrayInt(Vec<i64>),

    /// Array of floats (e.g., embeddings, feature vectors)
    ///
    /// Use this for:
    /// - ML embeddings (user vector, item vector)
    /// - Feature vectors
    /// - Score lists
    ArrayFloat(Vec<f64>),

    /// Array of strings (e.g., tags, categories)
    ///
    /// Use this for:
    /// - Tags (["electronics", "sale", "popular"])
    /// - Categories
    /// - List of labels
    ArrayString(Vec<String>),

    /// Date value (e.g., birth_date, signup_date)
    ///
    /// Use this for date-only values where time doesn't matter.
    /// For datetime with time component, use a timestamp Int or DateTime string.
    ///
    /// # Example
    /// ```rust,ignore
    /// use chrono::NaiveDate;
    /// let birth_date = FeatureValue::Date(NaiveDate::from_ymd_opt(1990, 1, 1).unwrap());
    /// ```
    Date(chrono::NaiveDate),

    /// JSON value (e.g., nested data, complex structures)
    ///
    /// Use this for:
    /// - Nested objects (user preferences, settings)
    /// - Complex metadata (product attributes)
    /// - Dynamic schemas
    ///
    /// IMPORTANT: Must be last because it matches everything
    /// (serde_json::Value accepts any valid JSON)
    ///
    /// # Example
    /// ```rust,ignore
    /// use serde_json::json;
    /// let metadata = FeatureValue::Json(json!({
    ///     "preferences": {"theme": "dark"},
    ///     "settings": {"notifications": true}
    /// }));
    /// ```
    Json(serde_json::Value),
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
/// - Timestamp when these features were computed (REQUIRED for point-in-time correctness)
///
/// ## Why timestamp is required
///
/// For ML correctness, we MUST know when each feature value was computed. This enables:
/// 1. **Point-in-time correct joins**: Get features as they existed at prediction time
/// 2. **No data leakage**: Never use future features in training
/// 3. **Training-serving parity**: Same features in training and production
///
/// # Examples
///
/// ```
/// use featureduck_core::{FeatureRow, EntityKey, FeatureValue};
/// use std::collections::HashMap;
/// use chrono::Utc;
///
/// let mut features = HashMap::new();
/// features.insert("clicks_7d".to_string(), FeatureValue::Int(42));
/// features.insert("purchases_30d".to_string(), FeatureValue::Int(3));
///
/// let row = FeatureRow {
///     entities: vec![EntityKey::new("user_id", "123")],
///     features,
///     timestamp: Utc::now(),  // When features were computed
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureRow {
    /// Entity identifiers for this row
    pub entities: Vec<EntityKey>,

    /// Feature name -> value mapping
    pub features: HashMap<String, FeatureValue>,

    /// Timestamp when these features were computed (REQUIRED)
    /// For point-in-time correct joins: get latest feature where timestamp <= event_time
    pub timestamp: DateTime<Utc>,
}

impl FeatureRow {
    /// Creates a new empty FeatureRow for the given entities and timestamp
    ///
    /// # Arguments
    /// * `entities` - Entity identifiers
    /// * `timestamp` - When these features were computed (required for point-in-time correctness)
    pub fn new(entities: Vec<EntityKey>, timestamp: DateTime<Utc>) -> Self {
        Self {
            entities,
            features: HashMap::new(),
            timestamp,
        }
    }

    /// Adds a feature to this row (builder pattern)
    pub fn with_feature(mut self, name: impl Into<String>, value: FeatureValue) -> Self {
        self.features.insert(name.into(), value);
        self
    }

    /// Adds a feature to this row (mutable)
    pub fn add_feature(&mut self, name: String, value: FeatureValue) {
        self.features.insert(name, value);
    }

    /// Gets a feature value by name
    pub fn get_feature(&self, name: &str) -> Option<&FeatureValue> {
        self.features.get(name)
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
        let timestamp = Utc::now();
        let row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], timestamp)
            .with_feature("clicks_7d", FeatureValue::Int(42))
            .with_feature("is_premium", FeatureValue::Bool(true));

        assert_eq!(row.entities.len(), 1);
        assert_eq!(row.features.len(), 2);
        assert_eq!(row.features.get("clicks_7d"), Some(&FeatureValue::Int(42)));
        assert_eq!(row.timestamp, timestamp);
    }

    #[test]
    fn test_feature_value_serialization() {
        // Test that FeatureValue serializes to clean JSON
        let value = FeatureValue::Int(42);
        let json = serde_json::to_string(&value).unwrap();
        assert_eq!(json, "42"); // Not {"Int": 42} because of #[serde(untagged)]
    }
}
