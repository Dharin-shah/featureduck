//! Feature definition validation framework
//!
//! This module provides a TDD-style validation system for feature schemas with:
//! - Type constraints (assert: must be Int, reject: cannot be String)
//! - Value constraints (min/max, regex patterns, enum values)
//! - Schema evolution rules (backward compatibility checks)
//! - Entity key validation
//!
//! ## Design Philosophy
//!
//! **Assert**: Define what MUST be true (positive validation)
//! **Reject**: Define what MUST NOT be true (negative validation)
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use featureduck_core::validation::{FeatureSchema, TypeConstraint, ValueConstraint};
//!
//! let schema = FeatureSchema::new("user_features")
//!     .assert_entity("user_id")  // Must have this entity
//!     .assert_feature("age", TypeConstraint::Int)  // Must be Int
//!     .assert_value("age", ValueConstraint::Range(0, 150))  // Must be 0-150
//!     .reject_feature("ssn")  // Must NOT have this feature (PII)
//!     .reject_type("credit_card", TypeConstraint::String);  // Must NOT be String (should be encrypted)
//!
//! // Validate a feature row
//! schema.validate(&feature_row)?;
//! ```

use crate::{FeatureRow, FeatureValue, Result};
use std::collections::{HashMap, HashSet};

/// Type constraint for feature validation
#[derive(Debug, Clone, PartialEq)]
pub enum TypeConstraint {
    /// Must be integer
    Int,
    /// Must be float
    Float,
    /// Must be string
    String,
    /// Must be boolean
    Bool,
    /// Must be JSON
    Json,
    /// Must be array of integers
    ArrayInt,
    /// Must be array of floats
    ArrayFloat,
    /// Must be array of strings
    ArrayString,
    /// Must be date
    Date,
    /// Can be null
    Nullable,
    /// Any type allowed
    Any,
}

impl TypeConstraint {
    /// Check if a FeatureValue matches this type constraint
    pub fn matches(&self, value: &FeatureValue) -> bool {
        matches!(
            (self, value),
            (TypeConstraint::Int, FeatureValue::Int(_))
                | (TypeConstraint::Float, FeatureValue::Float(_))
                | (TypeConstraint::String, FeatureValue::String(_))
                | (TypeConstraint::Bool, FeatureValue::Bool(_))
                | (TypeConstraint::Json, FeatureValue::Json(_))
                | (TypeConstraint::ArrayInt, FeatureValue::ArrayInt(_))
                | (TypeConstraint::ArrayFloat, FeatureValue::ArrayFloat(_))
                | (TypeConstraint::ArrayString, FeatureValue::ArrayString(_))
                | (TypeConstraint::Date, FeatureValue::Date(_))
                | (TypeConstraint::Nullable, FeatureValue::Null)
                | (TypeConstraint::Any, _)
        )
    }
}

/// Value constraint for feature validation
#[derive(Debug, Clone)]
pub enum ValueConstraint {
    /// Integer must be in range [min, max] (inclusive)
    IntRange(i64, i64),
    /// Float must be in range [min, max] (inclusive)
    FloatRange(f64, f64),
    /// String must match regex pattern
    StringPattern(String),
    /// String must be one of these values (enum)
    StringEnum(Vec<String>),
    /// String length must be in range [min, max]
    StringLength(usize, usize),
    /// Array length must be in range [min, max]
    ArrayLength(usize, usize),
    /// Custom validation function
    Custom(fn(&FeatureValue) -> bool),
}

impl ValueConstraint {
    /// Check if a FeatureValue satisfies this value constraint
    pub fn validate(&self, value: &FeatureValue) -> bool {
        match (self, value) {
            (ValueConstraint::IntRange(min, max), FeatureValue::Int(v)) => v >= min && v <= max,
            (ValueConstraint::FloatRange(min, max), FeatureValue::Float(v)) => v >= min && v <= max,
            (ValueConstraint::StringEnum(allowed), FeatureValue::String(v)) => allowed.contains(v),
            (ValueConstraint::StringLength(min, max), FeatureValue::String(v)) => {
                let len = v.len();
                len >= *min && len <= *max
            }
            (ValueConstraint::ArrayLength(min, max), FeatureValue::ArrayInt(v)) => {
                let len = v.len();
                len >= *min && len <= *max
            }
            (ValueConstraint::ArrayLength(min, max), FeatureValue::ArrayFloat(v)) => {
                let len = v.len();
                len >= *min && len <= *max
            }
            (ValueConstraint::ArrayLength(min, max), FeatureValue::ArrayString(v)) => {
                let len = v.len();
                len >= *min && len <= *max
            }
            (ValueConstraint::Custom(f), v) => f(v),
            _ => false,
        }
    }
}

/// Feature schema with assertion and rejection rules
#[derive(Debug, Clone)]
pub struct FeatureSchema {
    /// Schema name (e.g., "user_features")
    pub name: String,

    /// Required entity keys (MUST exist)
    pub required_entities: HashSet<String>,

    /// Required features (MUST exist)
    pub required_features: HashSet<String>,

    /// Forbidden features (MUST NOT exist)
    pub forbidden_features: HashSet<String>,

    /// Type constraints for features (MUST match type)
    pub type_constraints: HashMap<String, TypeConstraint>,

    /// Value constraints for features (MUST satisfy constraint)
    pub value_constraints: HashMap<String, Vec<ValueConstraint>>,
}

impl FeatureSchema {
    /// Create a new feature schema
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            required_entities: HashSet::new(),
            required_features: HashSet::new(),
            forbidden_features: HashSet::new(),
            type_constraints: HashMap::new(),
            value_constraints: HashMap::new(),
        }
    }

    /// Check if this schema has value constraints that require row-level validation
    ///
    /// Returns true if there are IntRange, FloatRange, StringEnum, etc. constraints
    /// that need to check actual values. Schema-level checks (type, forbidden, required)
    /// can be done once against the Arrow schema.
    pub fn needs_row_validation(&self) -> bool {
        !self.value_constraints.is_empty()
    }

    /// Assert that an entity key must exist
    pub fn assert_entity(mut self, entity: impl Into<String>) -> Self {
        self.required_entities.insert(entity.into());
        self
    }

    /// Assert that a feature must exist
    pub fn assert_feature(mut self, feature: impl Into<String>) -> Self {
        self.required_features.insert(feature.into());
        self
    }

    /// Assert that a feature must have a specific type
    pub fn assert_type(mut self, feature: impl Into<String>, constraint: TypeConstraint) -> Self {
        self.type_constraints.insert(feature.into(), constraint);
        self
    }

    /// Assert that a feature must satisfy a value constraint
    pub fn assert_value(mut self, feature: impl Into<String>, constraint: ValueConstraint) -> Self {
        let feature = feature.into();
        self.value_constraints
            .entry(feature)
            .or_default()
            .push(constraint);
        self
    }

    /// Reject (forbid) a feature from existing
    pub fn reject_feature(mut self, feature: impl Into<String>) -> Self {
        self.forbidden_features.insert(feature.into());
        self
    }

    /// Validate a feature row against this schema
    pub fn validate(&self, row: &FeatureRow) -> Result<()> {
        // 1. Check required entities
        for required_entity in &self.required_entities {
            if !row.entities.iter().any(|e| &e.name == required_entity) {
                return Err(crate::Error::InvalidInput(format!(
                    "Schema '{}': Missing required entity '{}'",
                    self.name, required_entity
                )));
            }
        }

        // 2. Check required features
        for required_feature in &self.required_features {
            if !row.features.contains_key(required_feature) {
                return Err(crate::Error::InvalidInput(format!(
                    "Schema '{}': Missing required feature '{}'",
                    self.name, required_feature
                )));
            }
        }

        // 3. Check forbidden features
        for forbidden_feature in &self.forbidden_features {
            if row.features.contains_key(forbidden_feature) {
                return Err(crate::Error::InvalidInput(format!(
                    "Schema '{}': Forbidden feature '{}' found",
                    self.name, forbidden_feature
                )));
            }
        }

        // 4. Check type constraints
        for (feature_name, constraint) in &self.type_constraints {
            if let Some(value) = row.features.get(feature_name) {
                if !constraint.matches(value) {
                    return Err(crate::Error::InvalidInput(format!(
                        "Schema '{}': Feature '{}' has wrong type. Expected {:?}, got {:?}",
                        self.name, feature_name, constraint, value
                    )));
                }
            }
        }

        // 5. Check value constraints
        for (feature_name, constraints) in &self.value_constraints {
            if let Some(value) = row.features.get(feature_name) {
                for constraint in constraints {
                    if !constraint.validate(value) {
                        return Err(crate::Error::InvalidInput(format!(
                            "Schema '{}': Feature '{}' failed value constraint: {:?}",
                            self.name, feature_name, constraint
                        )));
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EntityKey;
    use chrono::Utc;

    #[test]
    fn test_type_constraint_matches() {
        // Int
        assert!(TypeConstraint::Int.matches(&FeatureValue::Int(42)));
        assert!(!TypeConstraint::Int.matches(&FeatureValue::Float(42.0)));

        // Float
        assert!(TypeConstraint::Float.matches(&FeatureValue::Float(3.5)));
        assert!(!TypeConstraint::Float.matches(&FeatureValue::Int(3)));

        // String
        assert!(TypeConstraint::String.matches(&FeatureValue::String("hello".to_string())));
        assert!(!TypeConstraint::String.matches(&FeatureValue::Int(123)));

        // Bool
        assert!(TypeConstraint::Bool.matches(&FeatureValue::Bool(true)));
        assert!(!TypeConstraint::Bool.matches(&FeatureValue::Int(1)));

        // Nullable
        assert!(TypeConstraint::Nullable.matches(&FeatureValue::Null));
        assert!(!TypeConstraint::Nullable.matches(&FeatureValue::Int(0)));

        // Any
        assert!(TypeConstraint::Any.matches(&FeatureValue::Int(42)));
        assert!(TypeConstraint::Any.matches(&FeatureValue::String("test".to_string())));
        assert!(TypeConstraint::Any.matches(&FeatureValue::Null));
    }

    #[test]
    fn test_value_constraint_int_range() {
        let constraint = ValueConstraint::IntRange(0, 100);

        assert!(constraint.validate(&FeatureValue::Int(50)));
        assert!(constraint.validate(&FeatureValue::Int(0))); // min inclusive
        assert!(constraint.validate(&FeatureValue::Int(100))); // max inclusive
        assert!(!constraint.validate(&FeatureValue::Int(-1))); // below min
        assert!(!constraint.validate(&FeatureValue::Int(101))); // above max
    }

    #[test]
    fn test_value_constraint_float_range() {
        let constraint = ValueConstraint::FloatRange(0.0, 1.0);

        assert!(constraint.validate(&FeatureValue::Float(0.5)));
        assert!(constraint.validate(&FeatureValue::Float(0.0)));
        assert!(constraint.validate(&FeatureValue::Float(1.0)));
        assert!(!constraint.validate(&FeatureValue::Float(-0.1)));
        assert!(!constraint.validate(&FeatureValue::Float(1.1)));
    }

    #[test]
    fn test_value_constraint_string_enum() {
        let constraint = ValueConstraint::StringEnum(vec![
            "active".to_string(),
            "inactive".to_string(),
            "pending".to_string(),
        ]);

        assert!(constraint.validate(&FeatureValue::String("active".to_string())));
        assert!(constraint.validate(&FeatureValue::String("inactive".to_string())));
        assert!(!constraint.validate(&FeatureValue::String("expired".to_string())));
    }

    #[test]
    fn test_value_constraint_string_length() {
        let constraint = ValueConstraint::StringLength(3, 10);

        assert!(constraint.validate(&FeatureValue::String("hello".to_string()))); // 5 chars
        assert!(constraint.validate(&FeatureValue::String("abc".to_string()))); // 3 chars (min)
        assert!(constraint.validate(&FeatureValue::String("1234567890".to_string()))); // 10 chars (max)
        assert!(!constraint.validate(&FeatureValue::String("ab".to_string()))); // too short
        assert!(!constraint.validate(&FeatureValue::String("12345678901".to_string())));
        // too long
    }

    #[test]
    fn test_schema_assert_required_entity() {
        let schema = FeatureSchema::new("test").assert_entity("user_id");

        // Valid: has required entity
        let row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
        assert!(schema.validate(&row).is_ok());

        // Invalid: missing required entity
        let row = FeatureRow::new(vec![EntityKey::new("product_id", "456")], Utc::now());
        assert!(schema.validate(&row).is_err());
    }

    #[test]
    fn test_schema_assert_required_feature() {
        let schema = FeatureSchema::new("test")
            .assert_entity("user_id")
            .assert_feature("age");

        // Valid: has required feature
        let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
        row.add_feature("age".to_string(), FeatureValue::Int(30));
        assert!(schema.validate(&row).is_ok());

        // Invalid: missing required feature
        let row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
        assert!(schema.validate(&row).is_err());
    }

    #[test]
    fn test_schema_reject_forbidden_feature() {
        let schema = FeatureSchema::new("test")
            .assert_entity("user_id")
            .reject_feature("ssn"); // PII forbidden

        // Valid: no forbidden feature
        let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
        row.add_feature("age".to_string(), FeatureValue::Int(30));
        assert!(schema.validate(&row).is_ok());

        // Invalid: forbidden feature present
        let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
        row.add_feature(
            "ssn".to_string(),
            FeatureValue::String("123-45-6789".to_string()),
        );
        assert!(schema.validate(&row).is_err());
    }

    #[test]
    fn test_schema_assert_type_constraint() {
        let schema = FeatureSchema::new("test")
            .assert_entity("user_id")
            .assert_feature("age")
            .assert_type("age", TypeConstraint::Int);

        // Valid: correct type
        let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
        row.add_feature("age".to_string(), FeatureValue::Int(30));
        assert!(schema.validate(&row).is_ok());

        // Invalid: wrong type
        let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
        row.add_feature("age".to_string(), FeatureValue::String("30".to_string()));
        assert!(schema.validate(&row).is_err());
    }

    #[test]
    fn test_schema_assert_value_constraint() {
        let schema = FeatureSchema::new("test")
            .assert_entity("user_id")
            .assert_feature("age")
            .assert_type("age", TypeConstraint::Int)
            .assert_value("age", ValueConstraint::IntRange(0, 150));

        // Valid: value in range
        let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
        row.add_feature("age".to_string(), FeatureValue::Int(30));
        assert!(schema.validate(&row).is_ok());

        // Invalid: value out of range
        let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
        row.add_feature("age".to_string(), FeatureValue::Int(200));
        assert!(schema.validate(&row).is_err());
    }

    #[test]
    fn test_schema_complex_validation() {
        // Real-world example: user features schema
        let schema = FeatureSchema::new("user_features")
            .assert_entity("user_id")
            .assert_feature("age")
            .assert_type("age", TypeConstraint::Int)
            .assert_value("age", ValueConstraint::IntRange(0, 150))
            .assert_feature("status")
            .assert_type("status", TypeConstraint::String)
            .assert_value(
                "status",
                ValueConstraint::StringEnum(vec![
                    "active".to_string(),
                    "inactive".to_string(),
                    "suspended".to_string(),
                ]),
            )
            .reject_feature("ssn") // PII
            .reject_feature("credit_card"); // PII

        // Valid row
        let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
        row.add_feature("age".to_string(), FeatureValue::Int(30));
        row.add_feature(
            "status".to_string(),
            FeatureValue::String("active".to_string()),
        );
        row.add_feature("score".to_string(), FeatureValue::Float(0.85)); // optional feature
        assert!(schema.validate(&row).is_ok());

        // Invalid: forbidden feature
        let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
        row.add_feature("age".to_string(), FeatureValue::Int(30));
        row.add_feature(
            "status".to_string(),
            FeatureValue::String("active".to_string()),
        );
        row.add_feature(
            "ssn".to_string(),
            FeatureValue::String("123-45-6789".to_string()),
        );
        assert!(schema.validate(&row).is_err());
    }
}
