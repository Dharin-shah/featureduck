//! E2E tests for feature validation framework
//!
//! Tests the TDD-style validation system end-to-end:
//! - Type constraints (Int, Float, String, Bool, etc.)
//! - Value constraints (ranges, enums, patterns)
//! - Entity validation
//! - Forbidden feature rejection (PII prevention)
//! - Complex real-world schemas

use chrono::Utc;
use featureduck_core::{
    validation::{FeatureSchema, TypeConstraint, ValueConstraint},
    EntityKey, FeatureRow, FeatureValue,
};

// ============================================================================
// Basic Type Constraint Tests
// ============================================================================

#[test]
fn test_e2e_validation_int_type() {
    // Given: A schema requiring age to be Int
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .assert_feature("age")
        .assert_type("age", TypeConstraint::Int);

    // When: Valid int value
    let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row.add_feature("age".to_string(), FeatureValue::Int(30));

    // Then: Validation passes
    assert!(schema.validate(&row).is_ok());
}

#[test]
fn test_e2e_validation_float_type() {
    // Given: A schema requiring score to be Float
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .assert_feature("score")
        .assert_type("score", TypeConstraint::Float);

    // When: Valid float value
    let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row.add_feature("score".to_string(), FeatureValue::Float(0.85));

    // Then: Validation passes
    assert!(schema.validate(&row).is_ok());
}

#[test]
fn test_e2e_validation_string_type() {
    // Given: A schema requiring name to be String
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .assert_feature("name")
        .assert_type("name", TypeConstraint::String);

    // When: Valid string value
    let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row.add_feature(
        "name".to_string(),
        FeatureValue::String("Alice".to_string()),
    );

    // Then: Validation passes
    assert!(schema.validate(&row).is_ok());
}

#[test]
fn test_e2e_validation_bool_type() {
    // Given: A schema requiring is_active to be Bool
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .assert_feature("is_active")
        .assert_type("is_active", TypeConstraint::Bool);

    // When: Valid bool value
    let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row.add_feature("is_active".to_string(), FeatureValue::Bool(true));

    // Then: Validation passes
    assert!(schema.validate(&row).is_ok());
}

#[test]
fn test_e2e_validation_type_mismatch_fails() {
    // Given: A schema requiring age to be Int
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .assert_feature("age")
        .assert_type("age", TypeConstraint::Int);

    // When: Value is String instead of Int
    let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row.add_feature("age".to_string(), FeatureValue::String("30".to_string()));

    // Then: Validation fails
    let result = schema.validate(&row);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("wrong type"), "Error: {}", err);
}

// ============================================================================
// Value Constraint Tests
// ============================================================================

#[test]
fn test_e2e_validation_int_range_valid() {
    // Given: A schema with age range 0-150
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .assert_feature("age")
        .assert_type("age", TypeConstraint::Int)
        .assert_value("age", ValueConstraint::IntRange(0, 150));

    // When: Age is within range
    let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row.add_feature("age".to_string(), FeatureValue::Int(30));

    // Then: Validation passes
    assert!(schema.validate(&row).is_ok());
}

#[test]
fn test_e2e_validation_int_range_boundary() {
    // Given: A schema with age range 0-150
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .assert_feature("age")
        .assert_type("age", TypeConstraint::Int)
        .assert_value("age", ValueConstraint::IntRange(0, 150));

    // When: Age is at boundary (inclusive)
    let mut row_min = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row_min.add_feature("age".to_string(), FeatureValue::Int(0));

    let mut row_max = FeatureRow::new(vec![EntityKey::new("user_id", "456")], Utc::now());
    row_max.add_feature("age".to_string(), FeatureValue::Int(150));

    // Then: Both boundary values pass
    assert!(schema.validate(&row_min).is_ok());
    assert!(schema.validate(&row_max).is_ok());
}

#[test]
fn test_e2e_validation_int_range_out_of_bounds() {
    // Given: A schema with age range 0-150
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .assert_feature("age")
        .assert_type("age", TypeConstraint::Int)
        .assert_value("age", ValueConstraint::IntRange(0, 150));

    // When: Age is negative (below range)
    let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row.add_feature("age".to_string(), FeatureValue::Int(-1));

    // Then: Validation fails
    let result = schema.validate(&row);
    assert!(result.is_err());
}

#[test]
fn test_e2e_validation_float_range_valid() {
    // Given: A schema with score range 0.0-1.0
    let schema = FeatureSchema::new("ml_features")
        .assert_entity("user_id")
        .assert_feature("score")
        .assert_type("score", TypeConstraint::Float)
        .assert_value("score", ValueConstraint::FloatRange(0.0, 1.0));

    // When: Score is within range
    let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row.add_feature("score".to_string(), FeatureValue::Float(0.75));

    // Then: Validation passes
    assert!(schema.validate(&row).is_ok());
}

#[test]
fn test_e2e_validation_string_enum_valid() {
    // Given: A schema with status enum
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .assert_feature("status")
        .assert_type("status", TypeConstraint::String)
        .assert_value(
            "status",
            ValueConstraint::StringEnum(vec![
                "active".to_string(),
                "inactive".to_string(),
                "suspended".to_string(),
            ]),
        );

    // When: Status is valid enum value
    let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row.add_feature(
        "status".to_string(),
        FeatureValue::String("active".to_string()),
    );

    // Then: Validation passes
    assert!(schema.validate(&row).is_ok());
}

#[test]
fn test_e2e_validation_string_enum_invalid() {
    // Given: A schema with status enum
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .assert_feature("status")
        .assert_type("status", TypeConstraint::String)
        .assert_value(
            "status",
            ValueConstraint::StringEnum(vec![
                "active".to_string(),
                "inactive".to_string(),
                "suspended".to_string(),
            ]),
        );

    // When: Status is not in enum
    let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row.add_feature(
        "status".to_string(),
        FeatureValue::String("deleted".to_string()),
    );

    // Then: Validation fails
    let result = schema.validate(&row);
    assert!(result.is_err());
}

#[test]
fn test_e2e_validation_string_length_valid() {
    // Given: A schema with name length 1-100
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .assert_feature("name")
        .assert_type("name", TypeConstraint::String)
        .assert_value("name", ValueConstraint::StringLength(1, 100));

    // When: Name is within length bounds
    let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row.add_feature(
        "name".to_string(),
        FeatureValue::String("Alice".to_string()),
    );

    // Then: Validation passes
    assert!(schema.validate(&row).is_ok());
}

#[test]
fn test_e2e_validation_string_length_invalid() {
    // Given: A schema with name length 1-100
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .assert_feature("name")
        .assert_type("name", TypeConstraint::String)
        .assert_value("name", ValueConstraint::StringLength(1, 100));

    // When: Name is empty (too short)
    let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row.add_feature("name".to_string(), FeatureValue::String("".to_string()));

    // Then: Validation fails
    let result = schema.validate(&row);
    assert!(result.is_err());
}

// ============================================================================
// Entity Validation Tests
// ============================================================================

#[test]
fn test_e2e_validation_required_entity_present() {
    // Given: A schema requiring user_id entity
    let schema = FeatureSchema::new("user_features").assert_entity("user_id");

    // When: Row has required entity
    let row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());

    // Then: Validation passes
    assert!(schema.validate(&row).is_ok());
}

#[test]
fn test_e2e_validation_required_entity_missing() {
    // Given: A schema requiring user_id entity
    let schema = FeatureSchema::new("user_features").assert_entity("user_id");

    // When: Row has different entity
    let row = FeatureRow::new(vec![EntityKey::new("product_id", "456")], Utc::now());

    // Then: Validation fails
    let result = schema.validate(&row);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("Missing required entity"), "Error: {}", err);
}

#[test]
fn test_e2e_validation_multiple_entities() {
    // Given: A schema requiring both user_id and session_id
    let schema = FeatureSchema::new("session_features")
        .assert_entity("user_id")
        .assert_entity("session_id");

    // When: Row has both entities
    let row = FeatureRow::new(
        vec![
            EntityKey::new("user_id", "123"),
            EntityKey::new("session_id", "abc"),
        ],
        Utc::now(),
    );

    // Then: Validation passes
    assert!(schema.validate(&row).is_ok());
}

#[test]
fn test_e2e_validation_partial_entities_missing() {
    // Given: A schema requiring both user_id and session_id
    let schema = FeatureSchema::new("session_features")
        .assert_entity("user_id")
        .assert_entity("session_id");

    // When: Row has only one entity
    let row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());

    // Then: Validation fails
    let result = schema.validate(&row);
    assert!(result.is_err());
}

// ============================================================================
// Forbidden Feature Tests (PII Prevention)
// ============================================================================

#[test]
fn test_e2e_validation_reject_pii_ssn() {
    // Given: A schema that rejects SSN (PII)
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .reject_feature("ssn");

    // When: Row contains SSN
    let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row.add_feature(
        "ssn".to_string(),
        FeatureValue::String("123-45-6789".to_string()),
    );

    // Then: Validation fails
    let result = schema.validate(&row);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("Forbidden feature"), "Error: {}", err);
}

#[test]
fn test_e2e_validation_reject_multiple_pii() {
    // Given: A schema that rejects multiple PII fields
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .reject_feature("ssn")
        .reject_feature("credit_card")
        .reject_feature("password_hash");

    // When: Row contains credit card
    let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row.add_feature(
        "credit_card".to_string(),
        FeatureValue::String("4111-1111-1111-1111".to_string()),
    );

    // Then: Validation fails
    let result = schema.validate(&row);
    assert!(result.is_err());
}

#[test]
fn test_e2e_validation_allow_non_pii() {
    // Given: A schema that rejects PII but allows other features
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .reject_feature("ssn")
        .reject_feature("credit_card");

    // When: Row has only allowed features
    let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row.add_feature("age".to_string(), FeatureValue::Int(30));
    row.add_feature("score".to_string(), FeatureValue::Float(0.85));

    // Then: Validation passes
    assert!(schema.validate(&row).is_ok());
}

// ============================================================================
// Required Feature Tests
// ============================================================================

#[test]
fn test_e2e_validation_required_feature_present() {
    // Given: A schema requiring age feature
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .assert_feature("age");

    // When: Row has required feature
    let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row.add_feature("age".to_string(), FeatureValue::Int(30));

    // Then: Validation passes
    assert!(schema.validate(&row).is_ok());
}

#[test]
fn test_e2e_validation_required_feature_missing() {
    // Given: A schema requiring age feature
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .assert_feature("age");

    // When: Row is missing required feature
    let row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());

    // Then: Validation fails
    let result = schema.validate(&row);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("Missing required feature"), "Error: {}", err);
}

// ============================================================================
// Complex Real-World Schema Tests
// ============================================================================

#[test]
fn test_e2e_validation_user_features_schema() {
    // Given: A realistic user features schema
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        // Required features with type and value constraints
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
        // PII rejection
        .reject_feature("ssn")
        .reject_feature("credit_card")
        .reject_feature("password");

    // When: Valid row
    let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row.add_feature("age".to_string(), FeatureValue::Int(30));
    row.add_feature(
        "status".to_string(),
        FeatureValue::String("active".to_string()),
    );
    row.add_feature("score".to_string(), FeatureValue::Float(0.85));

    // Then: Validation passes
    assert!(schema.validate(&row).is_ok());
}

#[test]
fn test_e2e_validation_ml_features_schema() {
    // Given: ML features schema with score bounds
    let schema = FeatureSchema::new("ml_features")
        .assert_entity("user_id")
        .assert_feature("click_probability")
        .assert_type("click_probability", TypeConstraint::Float)
        .assert_value("click_probability", ValueConstraint::FloatRange(0.0, 1.0))
        .assert_feature("engagement_score")
        .assert_type("engagement_score", TypeConstraint::Float)
        .assert_value("engagement_score", ValueConstraint::FloatRange(0.0, 100.0));

    // When: Valid ML features
    let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row.add_feature("click_probability".to_string(), FeatureValue::Float(0.73));
    row.add_feature("engagement_score".to_string(), FeatureValue::Float(85.5));

    // Then: Validation passes
    assert!(schema.validate(&row).is_ok());
}

#[test]
fn test_e2e_validation_e_commerce_schema() {
    // Given: E-commerce product features schema
    let schema = FeatureSchema::new("product_features")
        .assert_entity("product_id")
        .assert_feature("category")
        .assert_type("category", TypeConstraint::String)
        .assert_feature("price")
        .assert_type("price", TypeConstraint::Float)
        .assert_value("price", ValueConstraint::FloatRange(0.01, 999999.99))
        .assert_feature("inventory")
        .assert_type("inventory", TypeConstraint::Int)
        .assert_value("inventory", ValueConstraint::IntRange(0, 1000000));

    // When: Valid product
    let mut row = FeatureRow::new(vec![EntityKey::new("product_id", "SKU123")], Utc::now());
    row.add_feature(
        "category".to_string(),
        FeatureValue::String("electronics".to_string()),
    );
    row.add_feature("price".to_string(), FeatureValue::Float(299.99));
    row.add_feature("inventory".to_string(), FeatureValue::Int(50));

    // Then: Validation passes
    assert!(schema.validate(&row).is_ok());
}

// ============================================================================
// Array Type Tests (JSON, Date, Array Types)
// ============================================================================

#[test]
fn test_e2e_validation_json_type() {
    // Given: A schema with JSON feature
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .assert_feature("preferences")
        .assert_type("preferences", TypeConstraint::Json);

    // When: Valid JSON value
    let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row.add_feature(
        "preferences".to_string(),
        FeatureValue::Json(serde_json::json!({"theme": "dark", "notifications": true})),
    );

    // Then: Validation passes
    assert!(schema.validate(&row).is_ok());
}

#[test]
fn test_e2e_validation_date_type() {
    // Given: A schema with Date feature
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .assert_feature("signup_date")
        .assert_type("signup_date", TypeConstraint::Date);

    // When: Valid date value
    let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row.add_feature(
        "signup_date".to_string(),
        FeatureValue::Date(chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()),
    );

    // Then: Validation passes
    assert!(schema.validate(&row).is_ok());
}

#[test]
fn test_e2e_validation_array_int_type() {
    // Given: A schema with ArrayInt feature
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .assert_feature("viewed_products")
        .assert_type("viewed_products", TypeConstraint::ArrayInt);

    // When: Valid array int value
    let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row.add_feature(
        "viewed_products".to_string(),
        FeatureValue::ArrayInt(vec![101, 102, 103]),
    );

    // Then: Validation passes
    assert!(schema.validate(&row).is_ok());
}

#[test]
fn test_e2e_validation_array_string_type() {
    // Given: A schema with ArrayString feature
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .assert_feature("tags")
        .assert_type("tags", TypeConstraint::ArrayString);

    // When: Valid array string value
    let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row.add_feature(
        "tags".to_string(),
        FeatureValue::ArrayString(vec!["premium".to_string(), "verified".to_string()]),
    );

    // Then: Validation passes
    assert!(schema.validate(&row).is_ok());
}

#[test]
fn test_e2e_validation_array_length() {
    // Given: A schema with array length constraint
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .assert_feature("tags")
        .assert_type("tags", TypeConstraint::ArrayString)
        .assert_value("tags", ValueConstraint::ArrayLength(1, 5));

    // When: Array within length bounds
    let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row.add_feature(
        "tags".to_string(),
        FeatureValue::ArrayString(vec!["tag1".to_string(), "tag2".to_string()]),
    );

    // Then: Validation passes
    assert!(schema.validate(&row).is_ok());
}

// ============================================================================
// Any Type and Nullable Tests
// ============================================================================

#[test]
fn test_e2e_validation_any_type() {
    // Given: A schema with Any type feature
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .assert_feature("metadata")
        .assert_type("metadata", TypeConstraint::Any);

    // When: Any value type
    let mut row_int = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row_int.add_feature("metadata".to_string(), FeatureValue::Int(42));

    let mut row_str = FeatureRow::new(vec![EntityKey::new("user_id", "456")], Utc::now());
    row_str.add_feature(
        "metadata".to_string(),
        FeatureValue::String("test".to_string()),
    );

    // Then: Both pass
    assert!(schema.validate(&row_int).is_ok());
    assert!(schema.validate(&row_str).is_ok());
}

#[test]
fn test_e2e_validation_nullable_type() {
    // Given: A schema with Nullable type feature
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .assert_feature("optional_field")
        .assert_type("optional_field", TypeConstraint::Nullable);

    // When: Null value
    let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row.add_feature("optional_field".to_string(), FeatureValue::Null);

    // Then: Validation passes
    assert!(schema.validate(&row).is_ok());
}

// ============================================================================
// Custom Validation Tests
// ============================================================================

#[test]
fn test_e2e_validation_custom_validator() {
    // Given: A schema with custom validator (even numbers only)
    fn is_even(value: &FeatureValue) -> bool {
        match value {
            FeatureValue::Int(v) => v % 2 == 0,
            _ => false,
        }
    }

    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .assert_feature("even_number")
        .assert_type("even_number", TypeConstraint::Int)
        .assert_value("even_number", ValueConstraint::Custom(is_even));

    // When: Even number
    let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row.add_feature("even_number".to_string(), FeatureValue::Int(42));

    // Then: Validation passes
    assert!(schema.validate(&row).is_ok());

    // When: Odd number
    let mut row_odd = FeatureRow::new(vec![EntityKey::new("user_id", "456")], Utc::now());
    row_odd.add_feature("even_number".to_string(), FeatureValue::Int(43));

    // Then: Validation fails
    assert!(schema.validate(&row_odd).is_err());
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn test_e2e_validation_empty_schema() {
    // Given: An empty schema (no constraints)
    let schema = FeatureSchema::new("empty_schema");

    // When: Any row
    let row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());

    // Then: Validation passes
    assert!(schema.validate(&row).is_ok());
}

#[test]
fn test_e2e_validation_multiple_value_constraints() {
    // Given: A feature with multiple value constraints
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .assert_feature("score")
        .assert_type("score", TypeConstraint::Int)
        .assert_value("score", ValueConstraint::IntRange(0, 100))
        .assert_value(
            "score",
            ValueConstraint::Custom(|v| {
                // Score must be divisible by 5
                match v {
                    FeatureValue::Int(i) => i % 5 == 0,
                    _ => false,
                }
            }),
        );

    // When: Score is 50 (in range and divisible by 5)
    let mut row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());
    row.add_feature("score".to_string(), FeatureValue::Int(50));

    // Then: Validation passes
    assert!(schema.validate(&row).is_ok());

    // When: Score is 51 (in range but not divisible by 5)
    let mut row_invalid = FeatureRow::new(vec![EntityKey::new("user_id", "456")], Utc::now());
    row_invalid.add_feature("score".to_string(), FeatureValue::Int(51));

    // Then: Validation fails
    assert!(schema.validate(&row_invalid).is_err());
}

#[test]
fn test_e2e_validation_optional_feature_with_constraints() {
    // Given: A schema with optional feature that has type constraint if present
    let schema = FeatureSchema::new("user_features")
        .assert_entity("user_id")
        .assert_type("optional_age", TypeConstraint::Int)
        .assert_value("optional_age", ValueConstraint::IntRange(0, 150));

    // When: Feature is not present (optional)
    let row = FeatureRow::new(vec![EntityKey::new("user_id", "123")], Utc::now());

    // Then: Validation passes (feature is optional)
    assert!(schema.validate(&row).is_ok());

    // When: Feature is present with valid value
    let mut row_with_age = FeatureRow::new(vec![EntityKey::new("user_id", "456")], Utc::now());
    row_with_age.add_feature("optional_age".to_string(), FeatureValue::Int(30));

    // Then: Validation passes
    assert!(schema.validate(&row_with_age).is_ok());
}
