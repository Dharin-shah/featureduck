"""Tests for Tecton-style validation API

Tests both decorator and fluent API styles for feature validation.
This is a comprehensive test suite for production-grade validation.
"""

import pytest
from featureduck.validation import (
    FeatureView,
    FeatureViewSchema,
    TypeConstraint,
    ValidationResult,
    ValidationError,
    ValidationRule,
    assert_type,
    assert_range,
    assert_enum,
    assert_feature,
    assert_length,
    assert_pattern,
    assert_array_length,
    assert_not_null,
    assert_custom,
    reject_pii,
    reject_feature,
    validate_feature_row,
    validate_batch,
)


# ====================================================================================
# Decorator Style Tests (Tecton-like)
# ====================================================================================


class TestDecoratorStyleBasic:
    """Test basic decorator-style feature view definitions"""

    def test_basic_feature_view(self):
        """Test basic decorator-style feature view definition"""

        @FeatureView(
            name="user_features",
            entities=["user_id"],
            description="User behavioral features"
        )
        class UserFeatures:
            pass

        assert hasattr(UserFeatures, '_feature_view_schema')
        schema = UserFeatures._feature_view_schema
        assert schema.name == "user_features"
        assert schema.entities == ["user_id"]
        assert schema.description == "User behavioral features"

    def test_assert_type_decorator(self):
        """Test @assert_type decorator preserves visual order"""

        @FeatureView(name="user_features", entities=["user_id"])
        @assert_type("age", int, "Age must be an integer")
        @assert_type("score", float)
        @assert_type("name", str)
        @assert_type("is_premium", bool)
        class UserFeatures:
            pass

        schema = UserFeatures._feature_view_schema
        assert len(schema.validation_rules) == 4

        # Check rules are in visual source order (age, score, name, is_premium)
        assert schema.validation_rules[0].feature_name == "age"
        assert schema.validation_rules[0].rule_type == "assert_type"
        assert schema.validation_rules[0].constraint == TypeConstraint.INT
        assert schema.validation_rules[0].reason == "Age must be an integer"

        assert schema.validation_rules[1].feature_name == "score"
        assert schema.validation_rules[1].constraint == TypeConstraint.FLOAT

        assert schema.validation_rules[2].feature_name == "name"
        assert schema.validation_rules[2].constraint == TypeConstraint.STRING

        assert schema.validation_rules[3].feature_name == "is_premium"
        assert schema.validation_rules[3].constraint == TypeConstraint.BOOL

    def test_assert_range_decorator(self):
        """Test @assert_range decorator preserves visual order"""

        @FeatureView(name="user_features", entities=["user_id"])
        @assert_range("age", min=0, max=150, reason="Age must be realistic")
        @assert_range("score", min=0.0, max=1.0)
        class UserFeatures:
            pass

        schema = UserFeatures._feature_view_schema
        assert len(schema.validation_rules) == 2

        # Visual order: age first, then score
        assert schema.validation_rules[0].feature_name == "age"
        assert schema.validation_rules[0].rule_type == "assert_range"
        assert schema.validation_rules[0].constraint == (0, 150)
        assert schema.validation_rules[0].reason == "Age must be realistic"

        assert schema.validation_rules[1].feature_name == "score"

    def test_reject_pii_decorator(self):
        """Test @reject_pii decorator for blocking sensitive data"""

        @FeatureView(name="user_features", entities=["user_id"])
        @reject_pii(["ssn", "credit_card", "email"], "PII not allowed")
        class UserFeatures:
            pass

        schema = UserFeatures._feature_view_schema
        assert len(schema.validation_rules) == 3

        # All three features should be rejected (order may vary due to decorator reversal)
        rejected_features = {rule.feature_name for rule in schema.validation_rules}
        assert rejected_features == {"ssn", "credit_card", "email"}

        for rule in schema.validation_rules:
            assert rule.rule_type == "reject"
            assert rule.reason == "PII not allowed"

    def test_complex_schema_decorator(self):
        """Test complex schema with multiple decorators (real-world example)"""

        @FeatureView(
            name="user_purchase_features",
            entities=["user_id"],
            description="User purchase behavioral features"
        )
        @assert_type("age", int)
        @assert_range("age", min=0, max=150)
        @assert_type("status", str)
        @assert_enum("status", ["active", "inactive", "suspended"])
        @assert_type("lifetime_value", float)
        @assert_range("lifetime_value", min=0.0, max=1000000.0)
        @reject_pii(["ssn", "credit_card"])
        class UserPurchaseFeatures:
            pass

        schema = UserPurchaseFeatures._feature_view_schema
        assert schema.name == "user_purchase_features"
        # 3 assert_type + 2 assert_range + 1 assert_enum + 2 reject = 8 rules
        assert len(schema.validation_rules) == 8


class TestDecoratorStyleAdvanced:
    """Test advanced decorator usage"""

    def test_assert_length_decorator(self):
        """Test @assert_length decorator"""

        @FeatureView(name="user_features", entities=["user_id"])
        @assert_length("name", min=1, max=100, reason="Name length check")
        class UserFeatures:
            pass

        schema = UserFeatures._feature_view_schema
        assert len(schema.validation_rules) == 1
        rule = schema.validation_rules[0]
        assert rule.rule_type == "assert_length"
        assert rule.constraint == (1, 100)

    def test_assert_pattern_decorator(self):
        """Test @assert_pattern decorator"""

        @FeatureView(name="user_features", entities=["user_id"])
        @assert_pattern("email", r"^[\w.+-]+@[\w-]+\.[\w.-]+$", reason="Invalid email")
        class UserFeatures:
            pass

        schema = UserFeatures._feature_view_schema
        assert len(schema.validation_rules) == 1
        rule = schema.validation_rules[0]
        assert rule.rule_type == "assert_pattern"

    def test_assert_pattern_invalid_regex_raises(self):
        """Test that invalid regex pattern raises ValueError"""
        with pytest.raises(ValueError, match="Invalid regex pattern"):
            @FeatureView(name="test", entities=[])
            @assert_pattern("field", r"[invalid(regex")  # Missing closing bracket
            class BadFeatures:
                pass

    def test_assert_array_length_decorator(self):
        """Test @assert_array_length decorator"""

        @FeatureView(name="user_features", entities=["user_id"])
        @assert_array_length("tags", min=1, max=10)
        class UserFeatures:
            pass

        schema = UserFeatures._feature_view_schema
        rule = schema.validation_rules[0]
        assert rule.rule_type == "assert_array_length"
        assert rule.constraint == (1, 10)

    def test_assert_not_null_decorator(self):
        """Test @assert_not_null decorator"""

        @FeatureView(name="user_features", entities=["user_id"])
        @assert_not_null("required_field", reason="Field is required")
        class UserFeatures:
            pass

        schema = UserFeatures._feature_view_schema
        rule = schema.validation_rules[0]
        assert rule.rule_type == "assert_not_null"

    def test_assert_custom_decorator(self):
        """Test @assert_custom decorator with custom validator"""

        def is_adult(age):
            return age >= 18

        @FeatureView(name="user_features", entities=["user_id"])
        @assert_custom("age", is_adult, reason="Must be adult")
        class UserFeatures:
            pass

        schema = UserFeatures._feature_view_schema
        rule = schema.validation_rules[0]
        assert rule.rule_type == "custom"
        assert callable(rule.constraint)

    def test_assert_feature_decorator(self):
        """Test @assert_feature decorator"""

        @FeatureView(name="user_features", entities=["user_id"])
        @assert_feature("required_feature", reason="This feature is required")
        class UserFeatures:
            pass

        schema = UserFeatures._feature_view_schema
        rule = schema.validation_rules[0]
        assert rule.rule_type == "assert_feature"


# ====================================================================================
# Fluent API Style Tests
# ====================================================================================


class TestFluentAPIBasic:
    """Test fluent API usage"""

    def test_basic_fluent_api(self):
        """Test basic fluent API usage"""
        view = FeatureView("user_features").with_entity("user_id")

        assert view.schema.name == "user_features"
        assert view.schema.entities == ["user_id"]

    def test_with_entities_multiple(self):
        """Test adding multiple entities"""
        view = FeatureView("features").with_entities("user_id", "product_id")

        assert view.schema.entities == ["user_id", "product_id"]

    def test_fluent_assert_type(self):
        """Test fluent API type assertions"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_type("age", int)
            .assert_type("score", float)
            .assert_type("name", str)
            .assert_type("is_premium", bool)
        )

        assert len(view.schema.validation_rules) == 4
        assert view.schema.validation_rules[0].constraint == TypeConstraint.INT
        assert view.schema.validation_rules[1].constraint == TypeConstraint.FLOAT
        assert view.schema.validation_rules[2].constraint == TypeConstraint.STRING
        assert view.schema.validation_rules[3].constraint == TypeConstraint.BOOL

    def test_fluent_assert_range(self):
        """Test fluent API range assertions"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_range("age", min=0, max=150, reason="Realistic age")
            .assert_range("score", min=0.0, max=1.0)
        )

        assert len(view.schema.validation_rules) == 2
        assert view.schema.validation_rules[0].constraint == (0, 150)
        assert view.schema.validation_rules[0].reason == "Realistic age"

    def test_fluent_reject_feature(self):
        """Test fluent API feature rejection"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .reject_feature("ssn", reason="PII not allowed")
            .reject_feature("credit_card", reason="PII not allowed")
        )

        assert len(view.schema.validation_rules) == 2
        for rule in view.schema.validation_rules:
            assert rule.rule_type == "reject"
            assert rule.reason == "PII not allowed"

    def test_fluent_reject_features_batch(self):
        """Test fluent API batch feature rejection"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .reject_features(["ssn", "credit_card", "email"], reason="PII")
        )

        assert len(view.schema.validation_rules) == 3

    def test_fluent_assert_enum(self):
        """Test fluent API enum assertions"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_enum("status", ["active", "inactive", "suspended"])
        )

        rule = view.schema.validation_rules[0]
        assert rule.rule_type == "assert_enum"
        assert rule.constraint == ["active", "inactive", "suspended"]

    def test_fluent_assert_length(self):
        """Test fluent API string length assertions"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_length("name", min=1, max=100)
        )

        rule = view.schema.validation_rules[0]
        assert rule.rule_type == "assert_length"
        assert rule.constraint == (1, 100)

    def test_fluent_assert_pattern(self):
        """Test fluent API pattern assertions"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_pattern("email", r"^[\w.+-]+@[\w-]+\.[\w.-]+$")
        )

        rule = view.schema.validation_rules[0]
        assert rule.rule_type == "assert_pattern"

    def test_fluent_assert_array_length(self):
        """Test fluent API array length assertions"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_array_length("tags", min=1, max=10)
        )

        rule = view.schema.validation_rules[0]
        assert rule.rule_type == "assert_array_length"
        assert rule.constraint == (1, 10)

    def test_fluent_assert_not_null(self):
        """Test fluent API not null assertions"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_not_null("required_field")
        )

        rule = view.schema.validation_rules[0]
        assert rule.rule_type == "assert_not_null"

    def test_fluent_assert_custom(self):
        """Test fluent API custom validators"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_custom("age", lambda x: x >= 18, reason="Must be adult")
        )

        rule = view.schema.validation_rules[0]
        assert rule.rule_type == "custom"
        assert callable(rule.constraint)

    def test_fluent_complex_chain(self):
        """Test complex fluent API chaining (real-world example)"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_type("age", int)
            .assert_range("age", 0, 150)
            .assert_type("status", str)
            .assert_enum("status", ["active", "inactive"])
            .reject_feature("ssn", reason="PII")
            .reject_feature("credit_card", reason="PII")
        )

        assert view.schema.name == "user_features"
        assert len(view.schema.validation_rules) == 6

    def test_fluent_with_strict_mode(self):
        """Test fluent API strict mode"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .with_strict_mode()
            .assert_type("age", int)
        )

        assert view.schema.strict_mode is True

    def test_fluent_with_type_constraint_enum(self):
        """Test using TypeConstraint enum directly in fluent API"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_type("metadata", TypeConstraint.JSON)
            .assert_type("tags", TypeConstraint.ARRAY_STRING)
        )

        assert len(view.schema.validation_rules) == 2
        assert view.schema.validation_rules[0].constraint == TypeConstraint.JSON
        assert view.schema.validation_rules[1].constraint == TypeConstraint.ARRAY_STRING


# ====================================================================================
# Schema Conversion Tests
# ====================================================================================


class TestSchemaConversion:
    """Test schema conversion to Rust format"""

    def test_to_rust_schema_basic(self):
        """Test converting Python schema to Rust format"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_type("age", int)
            .reject_feature("ssn")
        )

        rust_schema = view.schema.to_rust_schema()

        assert rust_schema["name"] == "user_features"
        assert "user_id" in rust_schema["required_entities"]
        assert "age" in rust_schema["type_constraints"]
        assert rust_schema["type_constraints"]["age"] == "Int"
        assert "ssn" in rust_schema["forbidden_features"]

    def test_to_rust_schema_with_ranges(self):
        """Test converting range constraints to Rust format"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_range("age", 0, 150)
            .assert_range("score", 0.0, 1.0)
        )

        rust_schema = view.schema.to_rust_schema()

        assert "age" in rust_schema["value_constraints"]
        assert len(rust_schema["value_constraints"]["age"]) == 1
        assert "IntRange" in rust_schema["value_constraints"]["age"][0]
        assert rust_schema["value_constraints"]["age"][0]["IntRange"] == [0, 150]

        assert "score" in rust_schema["value_constraints"]
        assert "FloatRange" in rust_schema["value_constraints"]["score"][0]

    def test_to_rust_schema_with_enum(self):
        """Test converting enum constraints to Rust format"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_enum("status", ["active", "inactive"])
        )

        rust_schema = view.schema.to_rust_schema()
        assert "StringEnum" in rust_schema["value_constraints"]["status"][0]

    def test_to_rust_schema_with_length(self):
        """Test converting length constraints to Rust format"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_length("name", 1, 100)
        )

        rust_schema = view.schema.to_rust_schema()
        assert "StringLength" in rust_schema["value_constraints"]["name"][0]

    def test_to_rust_schema_with_pattern(self):
        """Test converting pattern constraints to Rust format"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_pattern("email", r".*@.*")
        )

        rust_schema = view.schema.to_rust_schema()
        assert "Pattern" in rust_schema["value_constraints"]["email"][0]


# ====================================================================================
# Validation Tests - Entity Validation
# ====================================================================================


class TestEntityValidation:
    """Test entity validation"""

    def test_validate_missing_entity(self):
        """Test validation fails for missing required entity"""
        view = FeatureView("user_features").with_entity("user_id")

        feature_row = {
            "entities": [{"name": "product_id", "value": "123"}],
            "features": {}
        }

        is_valid, error = view.schema.validate(feature_row)

        assert not is_valid
        assert "Missing required entity: user_id" in error

    def test_validate_multiple_missing_entities(self):
        """Test validation reports all missing entities when collecting all errors"""
        view = (FeatureView("features")
            .with_entities("user_id", "product_id", "session_id")
        )

        feature_row = {
            "entities": [],
            "features": {}
        }

        result = view.schema.validate_detailed(feature_row, collect_all_errors=True)

        assert not result.is_valid
        assert len(result.errors) == 3

    def test_validate_entity_present(self):
        """Test validation passes when entity is present"""
        view = FeatureView("user_features").with_entity("user_id")

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {}
        }

        is_valid, error = view.schema.validate(feature_row)
        assert is_valid
        assert error is None


# ====================================================================================
# Validation Tests - Type Validation
# ====================================================================================


class TestTypeValidation:
    """Test type validation"""

    def test_validate_int_type_valid(self):
        """Test int type validation with valid value"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_type("age", int)
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"age": 30}
        }

        is_valid, error = view.schema.validate(feature_row)
        assert is_valid

    def test_validate_int_type_invalid(self):
        """Test int type validation with invalid value"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_type("age", int)
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"age": "thirty"}  # String instead of int
        }

        is_valid, error = view.schema.validate(feature_row)
        assert not is_valid
        assert "type error" in error

    def test_validate_float_type_accepts_int(self):
        """Test float type accepts integer values"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_type("score", float)
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"score": 10}  # int is acceptable for float
        }

        is_valid, error = view.schema.validate(feature_row)
        assert is_valid

    def test_validate_bool_type_strict(self):
        """Test bool type is strict (doesn't accept int)"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_type("is_active", bool)
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"is_active": 1}  # int instead of bool
        }

        is_valid, error = view.schema.validate(feature_row)
        assert not is_valid

    def test_validate_string_type(self):
        """Test string type validation"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_type("name", str)
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"name": "Alice"}
        }

        is_valid, error = view.schema.validate(feature_row)
        assert is_valid

    def test_validate_json_type(self):
        """Test JSON type validation accepts dict and list"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_type("metadata", TypeConstraint.JSON)
        )

        # Test dict
        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"metadata": {"key": "value"}}
        }
        is_valid, _ = view.schema.validate(feature_row)
        assert is_valid

        # Test list
        feature_row["features"]["metadata"] = [1, 2, 3]
        is_valid, _ = view.schema.validate(feature_row)
        assert is_valid

    def test_validate_array_int_type(self):
        """Test array int type validation"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_type("scores", TypeConstraint.ARRAY_INT)
        )

        # Valid array
        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"scores": [1, 2, 3]}
        }
        is_valid, _ = view.schema.validate(feature_row)
        assert is_valid

        # Invalid - contains string
        feature_row["features"]["scores"] = [1, "two", 3]
        is_valid, error = view.schema.validate(feature_row)
        assert not is_valid
        assert "array element" in error

    def test_validate_array_string_type(self):
        """Test array string type validation"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_type("tags", TypeConstraint.ARRAY_STRING)
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"tags": ["a", "b", "c"]}
        }
        is_valid, _ = view.schema.validate(feature_row)
        assert is_valid

    def test_validate_nullable_type(self):
        """Test nullable type accepts None"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_type("optional", TypeConstraint.NULLABLE)
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"optional": None}
        }

        is_valid, _ = view.schema.validate(feature_row)
        assert is_valid


# ====================================================================================
# Validation Tests - Value Validation
# ====================================================================================


class TestValueValidation:
    """Test value validation"""

    def test_validate_out_of_range_int(self):
        """Test validation fails for out-of-range int values"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_range("age", 0, 150)
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"age": 200}
        }

        is_valid, error = view.schema.validate(feature_row)

        assert not is_valid
        assert "out of range" in error or "[V301]" in error

    def test_validate_out_of_range_float(self):
        """Test validation fails for out-of-range float values"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_range("score", 0.0, 1.0)
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"score": 1.5}
        }

        is_valid, error = view.schema.validate(feature_row)

        assert not is_valid
        assert "out of range" in error or "[V301]" in error

    def test_validate_enum_invalid(self):
        """Test validation fails for invalid enum value"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_enum("status", ["active", "inactive"])
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"status": "deleted"}  # Not in enum
        }

        is_valid, error = view.schema.validate(feature_row)

        assert not is_valid
        assert "not in allowed values" in error

    def test_validate_enum_valid(self):
        """Test validation passes for valid enum value"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_enum("status", ["active", "inactive"])
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"status": "active"}
        }

        is_valid, _ = view.schema.validate(feature_row)
        assert is_valid

    def test_validate_string_length_too_short(self):
        """Test validation fails for string too short"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_length("name", min=3, max=100)
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"name": "Ab"}
        }

        is_valid, error = view.schema.validate(feature_row)
        assert not is_valid
        assert "length" in error

    def test_validate_string_length_too_long(self):
        """Test validation fails for string too long"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_length("name", min=1, max=10)
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"name": "Very Long Name Here"}
        }

        is_valid, error = view.schema.validate(feature_row)
        assert not is_valid
        assert "length" in error

    def test_validate_pattern_match(self):
        """Test pattern validation passes for matching value"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_pattern("email", r"^[\w.+-]+@[\w-]+\.[\w.-]+$")
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"email": "user@example.com"}
        }

        is_valid, _ = view.schema.validate(feature_row)
        assert is_valid

    def test_validate_pattern_no_match(self):
        """Test pattern validation fails for non-matching value"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_pattern("email", r"^[\w.+-]+@[\w-]+\.[\w.-]+$")
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"email": "not-an-email"}
        }

        is_valid, error = view.schema.validate(feature_row)
        assert not is_valid
        assert "does not match pattern" in error

    def test_validate_array_length_too_short(self):
        """Test validation fails for array too short"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_array_length("tags", min=2, max=10)
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"tags": ["single"]}
        }

        is_valid, error = view.schema.validate(feature_row)
        assert not is_valid
        assert "array length" in error

    def test_validate_array_length_too_long(self):
        """Test validation fails for array too long"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_array_length("tags", min=1, max=3)
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"tags": ["a", "b", "c", "d", "e"]}
        }

        is_valid, error = view.schema.validate(feature_row)
        assert not is_valid
        assert "array length" in error


# ====================================================================================
# Validation Tests - Forbidden Features
# ====================================================================================


class TestForbiddenFeatureValidation:
    """Test forbidden feature validation"""

    def test_validate_forbidden_feature(self):
        """Test validation fails for forbidden feature"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .reject_feature("ssn", reason="PII not allowed")
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"ssn": "123-45-6789"}
        }

        is_valid, error = view.schema.validate(feature_row)

        assert not is_valid
        assert "Forbidden feature 'ssn'" in error
        assert "PII not allowed" in error

    def test_validate_no_forbidden_feature(self):
        """Test validation passes when forbidden feature is absent"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .reject_feature("ssn")
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"age": 30}  # No SSN
        }

        is_valid, _ = view.schema.validate(feature_row)
        assert is_valid


# ====================================================================================
# Validation Tests - Required Features
# ====================================================================================


class TestRequiredFeatureValidation:
    """Test required feature validation"""

    def test_validate_missing_required_feature(self):
        """Test validation fails for missing required feature"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_feature("age", reason="Age is required")
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {}  # Missing age
        }

        is_valid, error = view.schema.validate(feature_row)

        assert not is_valid
        assert "Missing required feature 'age'" in error

    def test_validate_required_feature_present(self):
        """Test validation passes when required feature is present"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_feature("age")
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"age": 30}
        }

        is_valid, _ = view.schema.validate(feature_row)
        assert is_valid


# ====================================================================================
# Validation Tests - Custom Validators
# ====================================================================================


class TestCustomValidation:
    """Test custom validation"""

    def test_custom_validator_returns_true(self):
        """Test custom validator that returns True (pass)"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_custom("age", lambda x: x >= 18, reason="Must be adult")
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"age": 25}
        }

        is_valid, _ = view.schema.validate(feature_row)
        assert is_valid

    def test_custom_validator_returns_false(self):
        """Test custom validator that returns False (fail)"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_custom("age", lambda x: x >= 18, reason="Must be adult")
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"age": 15}
        }

        is_valid, error = view.schema.validate(feature_row)
        assert not is_valid
        assert "custom validation" in error
        assert "Must be adult" in error

    def test_custom_validator_returns_error_string(self):
        """Test custom validator that returns error string"""
        def validate_age(age):
            if age < 0:
                return "Age cannot be negative"
            if age > 150:
                return "Age seems unrealistic"
            return True

        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_custom("age", validate_age)
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"age": -5}
        }

        is_valid, error = view.schema.validate(feature_row)
        assert not is_valid
        assert "Age cannot be negative" in error

    def test_custom_validator_exception(self):
        """Test custom validator that raises exception"""
        def bad_validator(value):
            raise ValueError("Validator crashed!")

        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_custom("age", bad_validator)
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"age": 30}
        }

        is_valid, error = view.schema.validate(feature_row)
        assert not is_valid
        assert "custom validator error" in error


# ====================================================================================
# Validation Tests - Strict Mode
# ====================================================================================


class TestStrictModeValidation:
    """Test strict mode validation"""

    def test_strict_mode_rejects_unknown_features(self):
        """Test strict mode rejects unknown features"""
        view = (FeatureView("user_features", strict_mode=True)
            .with_entity("user_id")
            .assert_type("age", int)
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"age": 30, "unknown_field": "value"}
        }

        is_valid, error = view.schema.validate(feature_row)
        assert not is_valid
        assert "Unknown feature 'unknown_field'" in error
        assert "strict mode" in error

    def test_non_strict_mode_accepts_unknown_features(self):
        """Test non-strict mode accepts unknown features"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_type("age", int)
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"age": 30, "unknown_field": "value"}
        }

        is_valid, _ = view.schema.validate(feature_row)
        assert is_valid


# ====================================================================================
# Validation Tests - Success Cases
# ====================================================================================


class TestSuccessfulValidation:
    """Test successful validation scenarios"""

    def test_validate_success_full_schema(self):
        """Test validation succeeds for valid feature row with full schema"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_type("age", int)
            .assert_range("age", 0, 150)
            .assert_type("name", str)
            .assert_enum("status", ["active", "inactive"])
            .reject_feature("ssn")
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"age": 30, "name": "Alice", "status": "active"}
        }

        is_valid, error = view.schema.validate(feature_row)

        assert is_valid
        assert error is None

    def test_validate_success_with_null_optional_feature(self):
        """Test validation succeeds when optional feature is null"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_type("age", int)
            .assert_range("nickname", 0, 100)  # Optional field
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"age": 30, "nickname": None}
        }

        is_valid, _ = view.schema.validate(feature_row)
        assert is_valid


# ====================================================================================
# Validation Tests - Error Aggregation
# ====================================================================================


class TestErrorAggregation:
    """Test error aggregation functionality"""

    def test_collect_all_errors(self):
        """Test collecting all errors instead of failing fast"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_type("age", int)
            .assert_range("age", 0, 150)
            .assert_type("score", float)
            .reject_feature("ssn")
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {
                "age": "not-an-int",  # Type error
                "score": "not-a-float",  # Type error
                "ssn": "123-45-6789"  # Forbidden
            }
        }

        result = view.schema.validate_detailed(feature_row, collect_all_errors=True)

        assert not result.is_valid
        assert len(result.errors) >= 3

    def test_fail_fast_mode(self):
        """Test fail-fast mode stops at first error"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_type("age", int)
            .assert_type("score", float)
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {
                "age": "not-an-int",
                "score": "not-a-float"
            }
        }

        result = view.schema.validate_detailed(feature_row, collect_all_errors=False)

        assert not result.is_valid
        assert len(result.errors) == 1  # Only first error


# ====================================================================================
# Validation Tests - Batch Validation
# ====================================================================================


class TestBatchValidation:
    """Test batch validation functionality"""

    def test_batch_validation_all_valid(self):
        """Test batch validation with all valid rows"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_type("age", int)
        )

        rows = [
            {"entities": [{"name": "user_id", "value": "1"}], "features": {"age": 25}},
            {"entities": [{"name": "user_id", "value": "2"}], "features": {"age": 30}},
            {"entities": [{"name": "user_id", "value": "3"}], "features": {"age": 35}},
        ]

        results = view.schema.validate_batch(rows)

        assert all(r.is_valid for r in results)

    def test_batch_validation_some_invalid(self):
        """Test batch validation with some invalid rows"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_type("age", int)
        )

        rows = [
            {"entities": [{"name": "user_id", "value": "1"}], "features": {"age": 25}},
            {"entities": [{"name": "user_id", "value": "2"}], "features": {"age": "invalid"}},
            {"entities": [{"name": "user_id", "value": "3"}], "features": {"age": 35}},
        ]

        results = view.schema.validate_batch(rows)

        assert results[0].is_valid
        assert not results[1].is_valid
        assert results[2].is_valid

    def test_validate_batch_function(self):
        """Test validate_batch convenience function"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_type("age", int)
        )

        rows = [
            {"entities": [{"name": "user_id", "value": "1"}], "features": {"age": 25}},
            {"entities": [{"name": "user_id", "value": "2"}], "features": {"age": "invalid"}},
            {"entities": [{"name": "user_id", "value": "3"}], "features": {"age": 35}},
        ]

        valid_rows, invalid_rows = validate_batch(view, rows)

        assert len(valid_rows) == 2
        assert len(invalid_rows) == 1
        assert invalid_rows[0][0] == 1  # Index of invalid row

    def test_validate_batch_fail_fast(self):
        """Test validate_batch with fail_fast option"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_type("age", int)
        )

        rows = [
            {"entities": [{"name": "user_id", "value": "1"}], "features": {"age": 25}},
            {"entities": [{"name": "user_id", "value": "2"}], "features": {"age": "invalid"}},
            {"entities": [{"name": "user_id", "value": "3"}], "features": {"age": "also invalid"}},
        ]

        valid_rows, invalid_rows = validate_batch(view, rows, fail_fast=True)

        assert len(valid_rows) == 1
        assert len(invalid_rows) == 1  # Stopped at first invalid


# ====================================================================================
# Validation Tests - Convenience Functions
# ====================================================================================


class TestConvenienceFunctions:
    """Test convenience functions"""

    def test_validate_feature_row_function(self):
        """Test validate_feature_row convenience function"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_type("age", int)
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"age": 30}
        }

        result = validate_feature_row(view, feature_row)
        assert result.is_valid

    def test_validate_feature_row_raise_on_error(self):
        """Test validate_feature_row with raise_on_error"""
        view = (FeatureView("user_features")
            .with_entity("user_id")
            .assert_type("age", int)
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"age": "not-an-int"}
        }

        with pytest.raises(ValidationError) as exc_info:
            validate_feature_row(view, feature_row, raise_on_error=True)

        assert "type error" in str(exc_info.value)


# ====================================================================================
# TypeConstraint Tests
# ====================================================================================


class TestTypeConstraint:
    """Test TypeConstraint enum"""

    def test_type_constraint_enum_values(self):
        """Test TypeConstraint enum values"""
        assert TypeConstraint.INT.value == "Int"
        assert TypeConstraint.FLOAT.value == "Float"
        assert TypeConstraint.STRING.value == "String"
        assert TypeConstraint.BOOL.value == "Bool"
        assert TypeConstraint.JSON.value == "Json"
        assert TypeConstraint.ARRAY_INT.value == "ArrayInt"
        assert TypeConstraint.ARRAY_FLOAT.value == "ArrayFloat"
        assert TypeConstraint.ARRAY_STRING.value == "ArrayString"
        assert TypeConstraint.DATE.value == "Date"
        assert TypeConstraint.DATETIME.value == "Datetime"
        assert TypeConstraint.NULLABLE.value == "Nullable"
        assert TypeConstraint.ANY.value == "Any"


# ====================================================================================
# ValidationResult Tests
# ====================================================================================


class TestValidationResult:
    """Test ValidationResult class"""

    def test_validation_result_bool(self):
        """Test ValidationResult boolean conversion"""
        valid_result = ValidationResult(is_valid=True)
        assert bool(valid_result) is True

        invalid_result = ValidationResult(is_valid=False, errors=["Error"])
        assert bool(invalid_result) is False

    def test_validation_result_as_tuple(self):
        """Test ValidationResult as_tuple method"""
        result = ValidationResult(is_valid=False, errors=["First error", "Second error"])
        is_valid, error = result.as_tuple()

        assert is_valid is False
        assert error == "First error"

    def test_validation_result_error_message(self):
        """Test ValidationResult error_message property"""
        result = ValidationResult(is_valid=False, errors=["Error message"])
        assert result.error_message == "Error message"

        valid_result = ValidationResult(is_valid=True)
        assert valid_result.error_message is None


# ====================================================================================
# Edge Cases and Corner Cases
# ====================================================================================


class TestEdgeCases:
    """Test edge cases and corner cases"""

    def test_empty_feature_row(self):
        """Test validation with empty feature row"""
        view = FeatureView("features").with_entity("user_id")

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {}
        }

        is_valid, _ = view.schema.validate(feature_row)
        assert is_valid

    def test_missing_features_key(self):
        """Test validation when features key is missing"""
        view = FeatureView("features").with_entity("user_id")

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}]
            # No "features" key
        }

        is_valid, _ = view.schema.validate(feature_row)
        assert is_valid

    def test_missing_entities_key(self):
        """Test validation when entities key is missing"""
        view = FeatureView("features").with_entity("user_id")

        feature_row = {
            "features": {"age": 30}
            # No "entities" key
        }

        is_valid, error = view.schema.validate(feature_row)
        assert not is_valid
        assert "Missing required entity" in error

    def test_schema_without_rules(self):
        """Test schema without any validation rules"""
        view = FeatureView("features").with_entity("user_id")

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {"anything": "goes"}
        }

        is_valid, _ = view.schema.validate(feature_row)
        assert is_valid

    def test_feature_not_present_passes_type_check(self):
        """Test that type check passes when feature is not present"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_type("optional_field", int)
        )

        feature_row = {
            "entities": [{"name": "user_id", "value": "123"}],
            "features": {}  # optional_field not present
        }

        is_valid, _ = view.schema.validate(feature_row)
        assert is_valid

    def test_range_with_boundary_values(self):
        """Test range validation with boundary values"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_range("value", 0, 100)
        )

        # Test lower boundary
        row = {"entities": [{"name": "user_id", "value": "1"}], "features": {"value": 0}}
        assert view.schema.validate(row)[0]

        # Test upper boundary
        row = {"entities": [{"name": "user_id", "value": "1"}], "features": {"value": 100}}
        assert view.schema.validate(row)[0]

        # Test just outside boundaries
        row = {"entities": [{"name": "user_id", "value": "1"}], "features": {"value": -1}}
        assert not view.schema.validate(row)[0]

        row = {"entities": [{"name": "user_id", "value": "1"}], "features": {"value": 101}}
        assert not view.schema.validate(row)[0]

    def test_empty_string_length_validation(self):
        """Test string length validation with empty string"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_length("name", min=1, max=100)
        )

        row = {
            "entities": [{"name": "user_id", "value": "1"}],
            "features": {"name": ""}
        }

        is_valid, error = view.schema.validate(row)
        assert not is_valid

    def test_empty_array_length_validation(self):
        """Test array length validation with empty array"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_array_length("tags", min=1, max=10)
        )

        row = {
            "entities": [{"name": "user_id", "value": "1"}],
            "features": {"tags": []}
        }

        is_valid, error = view.schema.validate(row)
        assert not is_valid

    def test_unicode_strings(self):
        """Test validation with unicode strings"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_type("name", str)
            .assert_length("name", min=1, max=100)
        )

        row = {
            "entities": [{"name": "user_id", "value": "1"}],
            "features": {"name": "日本語テスト"}
        }

        is_valid, _ = view.schema.validate(row)
        assert is_valid

    def test_large_numbers(self):
        """Test validation with large numbers"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_type("big_number", int)
        )

        row = {
            "entities": [{"name": "user_id", "value": "1"}],
            "features": {"big_number": 10**18}
        }

        is_valid, _ = view.schema.validate(row)
        assert is_valid

    def test_negative_numbers(self):
        """Test validation with negative numbers"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_range("value", -100, 100)
        )

        row = {
            "entities": [{"name": "user_id", "value": "1"}],
            "features": {"value": -50}
        }

        is_valid, _ = view.schema.validate(row)
        assert is_valid

    def test_float_precision(self):
        """Test validation with float precision edge cases"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_range("score", 0.0, 1.0)
        )

        row = {
            "entities": [{"name": "user_id", "value": "1"}],
            "features": {"score": 0.9999999999}
        }

        is_valid, _ = view.schema.validate(row)
        assert is_valid


# ====================================================================================
# P0 Feature Tests - Error Codes
# ====================================================================================


class TestValidationErrorCodes:
    """Test ValidationErrorCode enum and structured error reporting"""

    def test_error_code_present_in_error_message(self):
        """Test that error codes are present in error messages"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_type("age", int)
        )

        row = {
            "entities": [{"name": "user_id", "value": "1"}],
            "features": {"age": "not-an-int"}
        }

        is_valid, error = view.schema.validate(row)
        assert not is_valid
        assert "[V201]" in error  # TYPE_MISMATCH

    def test_error_details_populated(self):
        """Test that error_details contains structured error info"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .reject_feature("ssn")
        )

        row = {
            "entities": [{"name": "user_id", "value": "1"}],
            "features": {"ssn": "123-45-6789"}
        }

        result = view.schema.validate_detailed(row)
        assert not result.is_valid
        assert len(result.error_details) == 1

        detail = result.error_details[0]
        assert detail.code.value == "V102"  # FORBIDDEN_FEATURE
        assert detail.feature_name == "ssn"
        assert detail.rule_type == "reject"

    def test_error_code_missing_entity(self):
        """Test V001 error code for missing entity"""
        view = FeatureView("features").with_entity("user_id")

        row = {
            "entities": [],
            "features": {"age": 30}
        }

        result = view.schema.validate_detailed(row)
        assert not result.is_valid
        assert result.error_code.value == "V001"

    def test_error_code_out_of_range(self):
        """Test V301 error code for out of range values"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_range("age", 0, 150)
        )

        row = {
            "entities": [{"name": "user_id", "value": "1"}],
            "features": {"age": 200}
        }

        result = view.schema.validate_detailed(row)
        assert not result.is_valid
        assert result.error_code.value == "V301"

    def test_error_code_pattern_mismatch(self):
        """Test V303 error code for pattern mismatch"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_pattern("email", r"^[a-z]+@[a-z]+\.[a-z]+$")
        )

        row = {
            "entities": [{"name": "user_id", "value": "1"}],
            "features": {"email": "invalid-email"}
        }

        result = view.schema.validate_detailed(row)
        assert not result.is_valid
        assert result.error_code.value == "V303"

    def test_error_details_to_dict(self):
        """Test ValidationErrorDetail.to_dict() serialization"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_type("age", int)
        )

        row = {
            "entities": [{"name": "user_id", "value": "1"}],
            "features": {"age": "not-int"}
        }

        result = view.schema.validate_detailed(row)
        detail_dict = result.error_details[0].to_dict()

        assert "code" in detail_dict
        assert "message" in detail_dict
        assert "feature_name" in detail_dict
        assert "rule_type" in detail_dict

    def test_validation_result_to_dict(self):
        """Test ValidationResult.to_dict() serialization"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_type("age", int)
        )

        row = {
            "entities": [{"name": "user_id", "value": "1"}],
            "features": {"age": "not-int"}
        }

        result = view.schema.validate_detailed(row)
        result_dict = result.to_dict()

        assert "is_valid" in result_dict
        assert "errors" in result_dict
        assert "warnings" in result_dict
        assert "error_details" in result_dict
        assert result_dict["is_valid"] is False


# ====================================================================================
# P0 Feature Tests - Validation Timeout
# ====================================================================================


class TestValidationTimeout:
    """Test validation timeout functionality"""

    def test_timeout_returns_timeout_error(self):
        """Test that timeout returns a timeout error"""
        import time

        # Create a custom validator that sleeps
        def slow_validator(value):
            time.sleep(0.5)  # Sleep 500ms
            return True

        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_custom("slow_field", slow_validator)
        )

        row = {
            "entities": [{"name": "user_id", "value": "1"}],
            "features": {"slow_field": "test"}
        }

        # Set a very short timeout (100ms < 500ms sleep)
        result = view.schema.validate_detailed(row, timeout_seconds=0.1)

        assert not result.is_valid
        assert result.error_code.value == "V901"  # TIMEOUT
        assert "timed out" in result.errors[0].lower()

    def test_fast_validation_completes_within_timeout(self):
        """Test that fast validation completes within timeout"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_type("age", int)
        )

        row = {
            "entities": [{"name": "user_id", "value": "1"}],
            "features": {"age": 30}
        }

        # Long timeout, should complete quickly
        result = view.schema.validate_detailed(row, timeout_seconds=5.0)

        assert result.is_valid

    def test_batch_validation_with_timeout(self):
        """Test batch validation with per-row timeout"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_type("age", int)
        )

        rows = [
            {"entities": [{"name": "user_id", "value": "1"}], "features": {"age": 25}},
            {"entities": [{"name": "user_id", "value": "2"}], "features": {"age": 30}},
            {"entities": [{"name": "user_id", "value": "3"}], "features": {"age": 35}},
        ]

        results = view.schema.validate_batch(rows, timeout_seconds=5.0)

        assert len(results) == 3
        assert all(r.is_valid for r in results)


# ====================================================================================
# P0 Feature Tests - Parallel Batch Validation
# ====================================================================================


class TestParallelBatchValidation:
    """Test parallel batch validation functionality"""

    def test_parallel_batch_same_results_as_sequential(self):
        """Test that parallel validation produces same results as sequential"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_type("age", int)
            .assert_range("age", 0, 150)
        )

        rows = [
            {"entities": [{"name": "user_id", "value": "1"}], "features": {"age": 25}},
            {"entities": [{"name": "user_id", "value": "2"}], "features": {"age": "invalid"}},
            {"entities": [{"name": "user_id", "value": "3"}], "features": {"age": 200}},  # Out of range
            {"entities": [{"name": "user_id", "value": "4"}], "features": {"age": 35}},
        ]

        # Sequential
        sequential_results = view.schema.validate_batch(rows, parallel=False)

        # Parallel
        parallel_results = view.schema.validate_batch(rows, parallel=True, max_workers=2)

        # Same number of results
        assert len(sequential_results) == len(parallel_results)

        # Same validity for each row
        for seq, par in zip(sequential_results, parallel_results):
            assert seq.is_valid == par.is_valid

    def test_parallel_batch_with_many_rows(self):
        """Test parallel validation with many rows"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_type("value", int)
        )

        # Create 100 rows
        rows = [
            {"entities": [{"name": "user_id", "value": str(i)}], "features": {"value": i}}
            for i in range(100)
        ]

        results = view.schema.validate_batch(rows, parallel=True, max_workers=4)

        assert len(results) == 100
        assert all(r.is_valid for r in results)

    def test_parallel_batch_with_errors(self):
        """Test parallel validation correctly identifies errors"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_type("age", int)
        )

        rows = [
            {"entities": [{"name": "user_id", "value": str(i)}], "features": {"age": "invalid" if i % 3 == 0 else i}}
            for i in range(30)
        ]

        results = view.schema.validate_batch(rows, parallel=True, max_workers=4)

        # Every 3rd row (0, 3, 6, ...) should be invalid
        for i, result in enumerate(results):
            if i % 3 == 0:
                assert not result.is_valid, f"Row {i} should be invalid"
            else:
                assert result.is_valid, f"Row {i} should be valid"


# ====================================================================================
# P0 Feature Tests - PII Safe Error Messages
# ====================================================================================


class TestPIISafeErrors:
    """Test that error messages do not contain PII (feature values)"""

    def test_range_error_no_value(self):
        """Test range error message does not include actual value"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_range("ssn_digits", 0, 999)
        )

        row = {
            "entities": [{"name": "user_id", "value": "1"}],
            "features": {"ssn_digits": 123456789}  # PII-like value
        }

        result = view.schema.validate_detailed(row)
        error_msg = result.errors[0]

        # Should NOT contain the actual value
        assert "123456789" not in error_msg
        # Should contain range info
        assert "[0, 999]" in error_msg or "0" in error_msg

    def test_type_error_no_value(self):
        """Test type error message does not include actual value"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_type("secret", str)
        )

        row = {
            "entities": [{"name": "user_id", "value": "1"}],
            "features": {"secret": 12345}  # Should be string
        }

        result = view.schema.validate_detailed(row)
        error_msg = result.errors[0]

        # Should NOT contain the actual value
        assert "12345" not in error_msg
        # Should mention the type
        assert "int" in error_msg or "string" in error_msg

    def test_pattern_error_no_value(self):
        """Test pattern error message does not include actual value"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_pattern("email", r"^[a-z]+@[a-z]+\.[a-z]+$")
        )

        row = {
            "entities": [{"name": "user_id", "value": "1"}],
            "features": {"email": "secret.user@company.com"}
        }

        result = view.schema.validate_detailed(row)
        error_msg = result.errors[0]

        # Should NOT contain the actual email
        assert "secret.user@company.com" not in error_msg
        # Should mention the pattern
        assert "pattern" in error_msg.lower()


# ====================================================================================
# P0 Feature Tests - Pre-compiled Regex
# ====================================================================================


class TestPrecompiledRegex:
    """Test that regex patterns are pre-compiled for performance"""

    def test_pattern_is_compiled_at_definition_time(self):
        """Test that invalid patterns raise error at definition time"""
        import re

        # This should raise ValueError immediately, not at validation time
        with pytest.raises(ValueError) as excinfo:
            view = (FeatureView("features")
                .with_entity("user_id")
                .assert_pattern("field", "[invalid(regex")  # Unclosed bracket
            )

        assert "Invalid regex pattern" in str(excinfo.value)

    def test_decorator_pattern_compiled_at_decoration_time(self):
        """Test that decorator also validates pattern at definition time"""
        import re

        with pytest.raises(ValueError) as excinfo:
            @FeatureView(name="features", entities=["user_id"])
            @assert_pattern("field", "[invalid(regex")
            class MyFeatures:
                pass

        assert "Invalid regex pattern" in str(excinfo.value)

    def test_valid_complex_pattern_works(self):
        """Test that complex but valid patterns work correctly"""
        view = (FeatureView("features")
            .with_entity("user_id")
            .assert_pattern("email", r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")
        )

        # Valid email
        row = {
            "entities": [{"name": "user_id", "value": "1"}],
            "features": {"email": "test.user@example.com"}
        }
        assert view.schema.validate(row)[0]

        # Invalid email
        row["features"]["email"] = "invalid"
        assert not view.schema.validate(row)[0]
