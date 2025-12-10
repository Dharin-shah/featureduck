"""Tecton-style API for feature validation

This module provides a user-friendly, declarative API for defining feature schemas
with validation rules. Inspired by Tecton's clean API design.

Example Usage:
    ```python
    from featureduck.validation import FeatureView, assert_type, assert_range, reject_pii

    @FeatureView(
        name="user_features",
        entities=["user_id"],
        description="User behavioral features"
    )
    @assert_type("age", int, "User age must be an integer")
    @assert_range("age", min=0, max=150, "Age must be between 0 and 150")
    @assert_type("email_verified", bool)
    @reject_pii(["ssn", "credit_card", "email"], "PII not allowed in feature store")
    class UserFeatures:
        '''User feature definition with validation'''
        pass

    # Or functional style:
    schema = (FeatureView("user_features")
        .with_entity("user_id")
        .assert_type("age", int)
        .assert_range("age", 0, 150)
        .reject_feature("ssn", reason="PII")
    )
    ```
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union, Pattern
from dataclasses import dataclass, field
from enum import Enum
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
import re


# ============================================================================
# Error Codes (P0-4)
# ============================================================================

class ValidationErrorCode(Enum):
    """Standardized error codes for debugging and monitoring.

    Error codes are prefixed with 'V' for validation errors.
    Use these codes for:
    - Debugging: Quickly identify error type
    - Monitoring: Track error rates by code
    - Documentation: Reference specific error types
    """
    # Entity errors (V0xx)
    MISSING_ENTITY = "V001"
    INVALID_ENTITY = "V002"

    # Feature presence errors (V1xx)
    MISSING_FEATURE = "V101"
    FORBIDDEN_FEATURE = "V102"
    UNKNOWN_FEATURE = "V103"  # Strict mode

    # Type errors (V2xx)
    TYPE_MISMATCH = "V201"
    ARRAY_ELEMENT_TYPE_MISMATCH = "V202"

    # Value constraint errors (V3xx)
    OUT_OF_RANGE = "V301"
    ENUM_MISMATCH = "V302"
    PATTERN_MISMATCH = "V303"
    LENGTH_VIOLATION = "V304"
    ARRAY_LENGTH_VIOLATION = "V305"
    NULL_VIOLATION = "V306"

    # Custom validation errors (V4xx)
    CUSTOM_VALIDATION_FAILED = "V401"
    CUSTOM_VALIDATOR_ERROR = "V402"

    # System errors (V9xx)
    TIMEOUT = "V901"
    INTERNAL_ERROR = "V999"


class TypeConstraint(Enum):
    """Type constraints for feature validation"""
    INT = "Int"
    FLOAT = "Float"
    STRING = "String"
    BOOL = "Bool"
    JSON = "Json"
    ARRAY_INT = "ArrayInt"
    ARRAY_FLOAT = "ArrayFloat"
    ARRAY_STRING = "ArrayString"
    DATE = "Date"
    DATETIME = "Datetime"
    NULLABLE = "Nullable"
    ANY = "Any"


# Python type to TypeConstraint mapping
_PYTHON_TYPE_MAP = {
    int: TypeConstraint.INT,
    float: TypeConstraint.FLOAT,
    str: TypeConstraint.STRING,
    bool: TypeConstraint.BOOL,
    list: TypeConstraint.ANY,  # Generic list, specific types use ARRAY_*
    dict: TypeConstraint.JSON,
    date: TypeConstraint.DATE,
    datetime: TypeConstraint.DATETIME,
}


def _python_type_to_constraint(py_type: type) -> TypeConstraint:
    """Convert Python type to TypeConstraint"""
    return _PYTHON_TYPE_MAP.get(py_type, TypeConstraint.ANY)


@dataclass
class ValidationErrorDetail:
    """Detailed validation error with code and context.

    Attributes:
        code: Standardized error code for programmatic handling
        message: Human-readable error message (PII-safe)
        feature_name: Name of the feature that failed validation (if applicable)
        rule_type: Type of validation rule that failed
    """
    code: ValidationErrorCode
    message: str
    feature_name: Optional[str] = None
    rule_type: Optional[str] = None

    def __str__(self) -> str:
        return f"[{self.code.value}] {self.message}"

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "code": self.code.value,
            "message": self.message,
            "feature_name": self.feature_name,
            "rule_type": self.rule_type,
        }


class ValidationError(Exception):
    """Exception raised when validation fails"""

    def __init__(
        self,
        message: str,
        errors: Optional[List[str]] = None,
        error_details: Optional[List[ValidationErrorDetail]] = None,
        code: ValidationErrorCode = ValidationErrorCode.INTERNAL_ERROR
    ):
        super().__init__(message)
        self.message = message
        self.errors = errors or [message]
        self.error_details = error_details or []
        self.code = code

    def __str__(self) -> str:
        return f"[{self.code.value}] {self.message}"


@dataclass
class ValidationRule:
    """A single validation rule"""
    feature_name: str
    rule_type: str  # "assert_type", "assert_range", "reject", etc.
    constraint: Any
    reason: Optional[str] = None

    def __repr__(self) -> str:
        return f"ValidationRule(feature='{self.feature_name}', type='{self.rule_type}')"


@dataclass
class ValidationResult:
    """Result of validating a feature row"""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    error_details: List[ValidationErrorDetail] = field(default_factory=list)

    def __bool__(self) -> bool:
        return self.is_valid

    @property
    def error_message(self) -> Optional[str]:
        """Return first error message or None"""
        return self.errors[0] if self.errors else None

    @property
    def error_code(self) -> Optional[ValidationErrorCode]:
        """Return first error code or None"""
        return self.error_details[0].code if self.error_details else None

    def as_tuple(self) -> Tuple[bool, Optional[str]]:
        """Return (is_valid, error_message) tuple for backwards compatibility"""
        return (self.is_valid, self.error_message)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "is_valid": self.is_valid,
            "errors": self.errors,
            "warnings": self.warnings,
            "error_details": [e.to_dict() for e in self.error_details],
        }


@dataclass
class FeatureViewSchema:
    """Feature view schema with validation rules

    This is the core schema object that gets built up through decorators
    or the fluent API.
    """
    name: str
    entities: List[str] = field(default_factory=list)
    description: str = ""
    validation_rules: List[ValidationRule] = field(default_factory=list)
    strict_mode: bool = False  # If True, reject unknown features

    def to_rust_schema(self) -> dict:
        """Convert to Rust FeatureSchema format

        Returns a dictionary that can be serialized to JSON and passed
        to the Rust validation layer.
        """
        rust_schema = {
            "name": self.name,
            "required_entities": set(self.entities),
            "required_features": set(),
            "forbidden_features": set(),
            "type_constraints": {},
            "value_constraints": {},
            "strict_mode": self.strict_mode
        }

        for rule in self.validation_rules:
            if rule.rule_type == "assert_type":
                rust_schema["type_constraints"][rule.feature_name] = rule.constraint.value
            elif rule.rule_type == "assert_range":
                if rule.feature_name not in rust_schema["value_constraints"]:
                    rust_schema["value_constraints"][rule.feature_name] = []
                min_val, max_val = rule.constraint
                constraint_type = "IntRange" if isinstance(min_val, int) and isinstance(max_val, int) else "FloatRange"
                rust_schema["value_constraints"][rule.feature_name].append({
                    constraint_type: [min_val, max_val]
                })
            elif rule.rule_type == "reject":
                rust_schema["forbidden_features"].add(rule.feature_name)
            elif rule.rule_type == "assert_feature":
                rust_schema["required_features"].add(rule.feature_name)
            elif rule.rule_type == "assert_enum":
                if rule.feature_name not in rust_schema["value_constraints"]:
                    rust_schema["value_constraints"][rule.feature_name] = []
                rust_schema["value_constraints"][rule.feature_name].append({
                    "StringEnum": rule.constraint
                })
            elif rule.rule_type == "assert_length":
                if rule.feature_name not in rust_schema["value_constraints"]:
                    rust_schema["value_constraints"][rule.feature_name] = []
                min_len, max_len = rule.constraint
                rust_schema["value_constraints"][rule.feature_name].append({
                    "StringLength": [min_len, max_len]
                })
            elif rule.rule_type == "assert_pattern":
                if rule.feature_name not in rust_schema["value_constraints"]:
                    rust_schema["value_constraints"][rule.feature_name] = []
                # Handle both compiled patterns and string patterns
                pattern_str = rule.constraint.pattern if isinstance(rule.constraint, Pattern) else rule.constraint
                rust_schema["value_constraints"][rule.feature_name].append({
                    "Pattern": pattern_str
                })
            elif rule.rule_type == "assert_array_length":
                if rule.feature_name not in rust_schema["value_constraints"]:
                    rust_schema["value_constraints"][rule.feature_name] = []
                min_len, max_len = rule.constraint
                rust_schema["value_constraints"][rule.feature_name].append({
                    "ArrayLength": [min_len, max_len]
                })

        return rust_schema

    def validate(self, feature_row: dict, collect_all_errors: bool = False) -> Tuple[bool, Optional[str]]:
        """Validate a feature row against this schema

        Args:
            feature_row: Dictionary with 'entities' and 'features' keys
            collect_all_errors: If True, collect all errors instead of failing fast

        Returns:
            Tuple of (is_valid, error_message)
        """
        result = self.validate_detailed(feature_row, collect_all_errors)
        return result.as_tuple()

    def validate_detailed(
        self,
        feature_row: dict,
        collect_all_errors: bool = False,
        timeout_seconds: Optional[float] = None
    ) -> ValidationResult:
        """Validate a feature row and return detailed results

        Args:
            feature_row: Dictionary with 'entities' and 'features' keys
            collect_all_errors: If True, collect all errors instead of failing fast
            timeout_seconds: Optional timeout for validation (default: None = no timeout)

        Returns:
            ValidationResult with is_valid, errors list, warnings, and error_details
        """
        # If timeout specified, run with timeout
        if timeout_seconds is not None:
            return self._validate_with_timeout(feature_row, collect_all_errors, timeout_seconds)

        return self._validate_impl(feature_row, collect_all_errors)

    def _validate_with_timeout(
        self,
        feature_row: dict,
        collect_all_errors: bool,
        timeout_seconds: float
    ) -> ValidationResult:
        """Run validation with timeout using ThreadPoolExecutor."""
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self._validate_impl, feature_row, collect_all_errors)
            try:
                return future.result(timeout=timeout_seconds)
            except FuturesTimeoutError:
                error_detail = ValidationErrorDetail(
                    code=ValidationErrorCode.TIMEOUT,
                    message=f"Validation timed out after {timeout_seconds}s",
                    feature_name=None,
                    rule_type=None
                )
                return ValidationResult(
                    is_valid=False,
                    errors=[str(error_detail)],
                    warnings=[],
                    error_details=[error_detail]
                )

    def _validate_impl(self, feature_row: dict, collect_all_errors: bool) -> ValidationResult:
        """Internal validation implementation."""
        errors: List[str] = []
        warnings: List[str] = []
        error_details: List[ValidationErrorDetail] = []

        def add_error(code: ValidationErrorCode, message: str, feature_name: str = None, rule_type: str = None):
            detail = ValidationErrorDetail(code, message, feature_name, rule_type)
            errors.append(str(detail))
            error_details.append(detail)

        # Check required entities
        row_entities = {e["name"] for e in feature_row.get("entities", [])}
        for required_entity in self.entities:
            if required_entity not in row_entities:
                add_error(
                    ValidationErrorCode.MISSING_ENTITY,
                    f"Missing required entity: {required_entity}",
                    feature_name=required_entity,
                    rule_type="entity"
                )
                if not collect_all_errors:
                    return ValidationResult(False, errors, warnings, error_details)

        # Get features
        features = feature_row.get("features", {})

        # Strict mode: check for unknown features
        if self.strict_mode:
            known_features = self._get_known_features()
            for feature_name in features:
                if feature_name not in known_features:
                    add_error(
                        ValidationErrorCode.UNKNOWN_FEATURE,
                        f"Unknown feature '{feature_name}' (strict mode enabled)",
                        feature_name=feature_name,
                        rule_type="strict_mode"
                    )
                    if not collect_all_errors:
                        return ValidationResult(False, errors, warnings, error_details)

        # Check validation rules
        for rule in self.validation_rules:
            error_result = self._validate_rule(rule, features)
            if error_result:
                error_code, error_msg = error_result
                add_error(error_code, error_msg, rule.feature_name, rule.rule_type)
                if not collect_all_errors:
                    return ValidationResult(False, errors, warnings, error_details)

        return ValidationResult(len(errors) == 0, errors, warnings, error_details)

    def _get_known_features(self) -> Set[str]:
        """Get set of known feature names from validation rules"""
        known = set()
        for rule in self.validation_rules:
            if rule.rule_type not in ("reject",):  # Rejected features aren't "known"
                known.add(rule.feature_name)
        return known

    def _validate_rule(
        self,
        rule: ValidationRule,
        features: Dict[str, Any]
    ) -> Optional[Tuple[ValidationErrorCode, str]]:
        """Validate a single rule against features.

        Returns:
            None if validation passes, or (error_code, error_message) tuple if it fails.
            NOTE: Error messages are PII-safe - they don't include actual feature values.
        """
        feature_name = rule.feature_name

        if rule.rule_type == "reject":
            if feature_name in features:
                reason = rule.reason or "Feature is forbidden"
                return (ValidationErrorCode.FORBIDDEN_FEATURE,
                        f"Forbidden feature '{feature_name}': {reason}")
            return None

        if rule.rule_type == "assert_feature":
            if feature_name not in features:
                reason = rule.reason or "Feature is required"
                return (ValidationErrorCode.MISSING_FEATURE,
                        f"Missing required feature '{feature_name}': {reason}")
            return None

        # For remaining rules, feature must be present
        if feature_name not in features:
            return None  # Not present, no validation needed

        value = features[feature_name]

        # Handle None values
        if value is None:
            # Check if nullable is allowed
            if rule.rule_type == "assert_type" and rule.constraint == TypeConstraint.NULLABLE:
                return None
            # For other rules, None is acceptable unless the feature is required
            return None

        if rule.rule_type == "assert_type":
            return self._validate_type(feature_name, value, rule.constraint, rule.reason)

        if rule.rule_type == "assert_range":
            min_val, max_val = rule.constraint
            # Handle type mismatch gracefully
            if not isinstance(value, (int, float)):
                return (ValidationErrorCode.TYPE_MISMATCH,
                        f"Feature '{feature_name}' expected numeric type for range check, got {type(value).__name__}")
            if not (min_val <= value <= max_val):
                reason = rule.reason or f"must be between {min_val} and {max_val}"
                # PII-SAFE: Don't include actual value
                return (ValidationErrorCode.OUT_OF_RANGE,
                        f"Feature '{feature_name}' value out of range [{min_val}, {max_val}]: {reason}")
            return None

        if rule.rule_type == "assert_enum":
            allowed_values = rule.constraint
            if value not in allowed_values:
                reason = rule.reason or f"must be one of {allowed_values}"
                # PII-SAFE: Don't include actual value
                return (ValidationErrorCode.ENUM_MISMATCH,
                        f"Feature '{feature_name}' value not in allowed values {allowed_values}: {reason}")
            return None

        if rule.rule_type == "assert_length":
            min_len, max_len = rule.constraint
            if not isinstance(value, str):
                return (ValidationErrorCode.TYPE_MISMATCH,
                        f"Feature '{feature_name}' expected string for length check, got {type(value).__name__}")
            actual_len = len(value)
            if not (min_len <= actual_len <= max_len):
                reason = rule.reason or f"length must be between {min_len} and {max_len}"
                # PII-SAFE: Include length but not actual value
                return (ValidationErrorCode.LENGTH_VIOLATION,
                        f"Feature '{feature_name}' length {actual_len} not in range [{min_len}, {max_len}]: {reason}")
            return None

        if rule.rule_type == "assert_pattern":
            # P0-2: Use pre-compiled pattern if available
            pattern = rule.constraint
            if not isinstance(value, str):
                return (ValidationErrorCode.TYPE_MISMATCH,
                        f"Feature '{feature_name}' expected string for pattern check, got {type(value).__name__}")

            # Check if pattern is pre-compiled (Pattern object) or string
            if isinstance(pattern, Pattern):
                match_result = pattern.match(value)
            else:
                match_result = re.match(pattern, value)

            if not match_result:
                # Get pattern string for error message
                pattern_str = pattern.pattern if isinstance(pattern, Pattern) else pattern
                reason = rule.reason or f"must match pattern"
                # PII-SAFE: Don't include actual value
                return (ValidationErrorCode.PATTERN_MISMATCH,
                        f"Feature '{feature_name}' does not match pattern '{pattern_str}': {reason}")
            return None

        if rule.rule_type == "assert_array_length":
            min_len, max_len = rule.constraint
            if not isinstance(value, (list, tuple)):
                return (ValidationErrorCode.TYPE_MISMATCH,
                        f"Feature '{feature_name}' expected array for length check, got {type(value).__name__}")
            actual_len = len(value)
            if not (min_len <= actual_len <= max_len):
                reason = rule.reason or f"array length must be between {min_len} and {max_len}"
                return (ValidationErrorCode.ARRAY_LENGTH_VIOLATION,
                        f"Feature '{feature_name}' array length {actual_len} not in range [{min_len}, {max_len}]: {reason}")
            return None

        if rule.rule_type == "assert_not_null":
            if value is None:
                reason = rule.reason or "must not be null"
                return (ValidationErrorCode.NULL_VIOLATION,
                        f"Feature '{feature_name}' is null: {reason}")
            return None

        if rule.rule_type == "custom":
            validator_fn = rule.constraint
            try:
                result = validator_fn(value)
                if result is False:
                    reason = rule.reason or "custom validation failed"
                    return (ValidationErrorCode.CUSTOM_VALIDATION_FAILED,
                            f"Feature '{feature_name}' failed custom validation: {reason}")
                elif isinstance(result, str):
                    # Validator returned an error message
                    return (ValidationErrorCode.CUSTOM_VALIDATION_FAILED,
                            f"Feature '{feature_name}': {result}")
            except Exception as e:
                return (ValidationErrorCode.CUSTOM_VALIDATOR_ERROR,
                        f"Feature '{feature_name}' custom validator error: {str(e)}")
            return None

        return None  # Unknown rule type - pass through

    def _validate_type(
        self,
        feature_name: str,
        value: Any,
        constraint: TypeConstraint,
        reason: Optional[str]
    ) -> Optional[Tuple[ValidationErrorCode, str]]:
        """Validate a value against a type constraint.

        Returns:
            None if validation passes, or (error_code, error_message) tuple if it fails.
        """
        if constraint == TypeConstraint.ANY or constraint == TypeConstraint.NULLABLE:
            return None

        type_checks = {
            TypeConstraint.INT: (int, "integer"),
            TypeConstraint.FLOAT: ((int, float), "number"),  # int is acceptable for float
            TypeConstraint.STRING: (str, "string"),
            TypeConstraint.BOOL: (bool, "boolean"),
            TypeConstraint.JSON: ((dict, list), "JSON object/array"),
            TypeConstraint.DATE: ((date, str), "date"),  # Accept string for ISO dates
            TypeConstraint.DATETIME: ((datetime, str), "datetime"),
            TypeConstraint.ARRAY_INT: (list, "integer array"),
            TypeConstraint.ARRAY_FLOAT: (list, "float array"),
            TypeConstraint.ARRAY_STRING: (list, "string array"),
        }

        if constraint not in type_checks:
            return None

        expected_type, type_name = type_checks[constraint]

        # Special handling for bool - must be strictly bool, not int
        if constraint == TypeConstraint.BOOL:
            if not isinstance(value, bool):
                reason_msg = reason or f"expected {type_name}"
                return (ValidationErrorCode.TYPE_MISMATCH,
                        f"Feature '{feature_name}' type error: got {type(value).__name__}, {reason_msg}")
            return None

        if not isinstance(value, expected_type):
            reason_msg = reason or f"expected {type_name}"
            return (ValidationErrorCode.TYPE_MISMATCH,
                    f"Feature '{feature_name}' type error: got {type(value).__name__}, {reason_msg}")

        # Additional array element type validation
        if constraint in (TypeConstraint.ARRAY_INT, TypeConstraint.ARRAY_FLOAT, TypeConstraint.ARRAY_STRING):
            element_checks = {
                TypeConstraint.ARRAY_INT: (int, "integer"),
                TypeConstraint.ARRAY_FLOAT: ((int, float), "number"),
                TypeConstraint.ARRAY_STRING: (str, "string"),
            }
            elem_type, elem_name = element_checks[constraint]
            for i, elem in enumerate(value):
                if not isinstance(elem, elem_type):
                    return (ValidationErrorCode.ARRAY_ELEMENT_TYPE_MISMATCH,
                            f"Feature '{feature_name}' array element [{i}] type error: got {type(elem).__name__}, expected {elem_name}")

        return None

    def validate_batch(
        self,
        feature_rows: List[dict],
        collect_all_errors: bool = False,
        parallel: bool = False,
        max_workers: int = 4,
        timeout_seconds: Optional[float] = None
    ) -> List[ValidationResult]:
        """Validate multiple feature rows

        Args:
            feature_rows: List of feature row dictionaries
            collect_all_errors: If True, collect all errors for each row
            parallel: If True, validate rows in parallel using ThreadPoolExecutor
            max_workers: Number of worker threads for parallel validation
            timeout_seconds: Optional timeout per row (only used with parallel=True)

        Returns:
            List of ValidationResult objects
        """
        if parallel:
            return self._validate_batch_parallel(
                feature_rows, collect_all_errors, max_workers, timeout_seconds
            )
        return [self.validate_detailed(row, collect_all_errors, timeout_seconds) for row in feature_rows]

    def _validate_batch_parallel(
        self,
        feature_rows: List[dict],
        collect_all_errors: bool,
        max_workers: int,
        timeout_seconds: Optional[float]
    ) -> List[ValidationResult]:
        """Validate rows in parallel using ThreadPoolExecutor.

        This provides significant speedup for large batches (1000+ rows).
        """
        def validate_row(row: dict) -> ValidationResult:
            return self.validate_detailed(row, collect_all_errors, timeout_seconds)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(validate_row, feature_rows))
        return results


class FeatureView:
    """Feature view decorator/builder

    Can be used as:
    1. Class decorator: @FeatureView(name="...")
    2. Fluent builder: FeatureView("name").with_entity("user_id").assert_type(...)
    """

    def __init__(
        self,
        name: str,
        entities: Optional[List[str]] = None,
        description: str = "",
        strict_mode: bool = False
    ):
        self.schema = FeatureViewSchema(
            name=name,
            entities=entities or [],
            description=description,
            strict_mode=strict_mode
        )

    def __call__(self, cls):
        """Decorator mode: @FeatureView(name="...")

        This should be the FIRST decorator (outermost) in the chain:
            @FeatureView(name="...")  # <-- First (outermost)
            @assert_type(...)
            @assert_range(...)
            class MyFeatures:
                pass
        """
        # If the class already has a schema (from other decorators applied first),
        # merge their rules into our schema
        if hasattr(cls, '_feature_view_schema'):
            existing_schema = cls._feature_view_schema
            # Reverse the rules since decorators execute bottom-to-top
            # We want to preserve the visual order in the source code
            self.schema.validation_rules = list(reversed(existing_schema.validation_rules))

        # Attach schema to the class
        cls._feature_view_schema = self.schema
        return cls

    # ============================================================================
    # Fluent API Methods
    # ============================================================================

    def with_entity(self, entity_name: str) -> 'FeatureView':
        """Fluent API: add an entity"""
        self.schema.entities.append(entity_name)
        return self

    def with_entities(self, *entity_names: str) -> 'FeatureView':
        """Fluent API: add multiple entities"""
        self.schema.entities.extend(entity_names)
        return self

    def with_strict_mode(self, strict: bool = True) -> 'FeatureView':
        """Fluent API: enable strict mode (reject unknown features)"""
        self.schema.strict_mode = strict
        return self

    def assert_type(
        self,
        feature_name: str,
        feature_type: Union[type, TypeConstraint],
        reason: Optional[str] = None
    ) -> 'FeatureView':
        """Fluent API: assert feature type"""
        constraint = _python_type_to_constraint(feature_type) if isinstance(feature_type, type) else feature_type
        self.schema.validation_rules.append(
            ValidationRule(feature_name, "assert_type", constraint, reason)
        )
        return self

    def assert_range(
        self,
        feature_name: str,
        min: Union[int, float],
        max: Union[int, float],
        reason: Optional[str] = None
    ) -> 'FeatureView':
        """Fluent API: assert value range"""
        self.schema.validation_rules.append(
            ValidationRule(feature_name, "assert_range", (min, max), reason)
        )
        return self

    def assert_enum(
        self,
        feature_name: str,
        values: List[str],
        reason: Optional[str] = None
    ) -> 'FeatureView':
        """Fluent API: assert string enum"""
        self.schema.validation_rules.append(
            ValidationRule(feature_name, "assert_enum", values, reason)
        )
        return self

    def assert_feature(
        self,
        feature_name: str,
        reason: Optional[str] = None
    ) -> 'FeatureView':
        """Fluent API: assert feature exists"""
        self.schema.validation_rules.append(
            ValidationRule(feature_name, "assert_feature", None, reason)
        )
        return self

    def assert_length(
        self,
        feature_name: str,
        min: int = 0,
        max: int = 2**31,
        reason: Optional[str] = None
    ) -> 'FeatureView':
        """Fluent API: assert string length"""
        self.schema.validation_rules.append(
            ValidationRule(feature_name, "assert_length", (min, max), reason)
        )
        return self

    def assert_pattern(
        self,
        feature_name: str,
        pattern: str,
        reason: Optional[str] = None
    ) -> 'FeatureView':
        """Fluent API: assert string matches regex pattern

        NOTE: Pattern is pre-compiled for performance. This means the regex
        is only compiled once, not on every validation call.
        """
        # P0-2: Pre-compile pattern for performance
        try:
            compiled_pattern = re.compile(pattern)
        except re.error as e:
            raise ValueError(f"Invalid regex pattern '{pattern}': {e}")
        self.schema.validation_rules.append(
            ValidationRule(feature_name, "assert_pattern", compiled_pattern, reason)
        )
        return self

    def assert_array_length(
        self,
        feature_name: str,
        min: int = 0,
        max: int = 2**31,
        reason: Optional[str] = None
    ) -> 'FeatureView':
        """Fluent API: assert array length"""
        self.schema.validation_rules.append(
            ValidationRule(feature_name, "assert_array_length", (min, max), reason)
        )
        return self

    def assert_not_null(
        self,
        feature_name: str,
        reason: Optional[str] = None
    ) -> 'FeatureView':
        """Fluent API: assert feature is not null"""
        self.schema.validation_rules.append(
            ValidationRule(feature_name, "assert_not_null", None, reason)
        )
        return self

    def assert_custom(
        self,
        feature_name: str,
        validator: Callable[[Any], Union[bool, str]],
        reason: Optional[str] = None
    ) -> 'FeatureView':
        """Fluent API: custom validation function

        The validator function receives the feature value and should return:
        - True: validation passed
        - False: validation failed (use `reason` for error message)
        - str: validation failed with custom error message
        """
        self.schema.validation_rules.append(
            ValidationRule(feature_name, "custom", validator, reason)
        )
        return self

    def reject_feature(
        self,
        feature_name: str,
        reason: Optional[str] = None
    ) -> 'FeatureView':
        """Fluent API: reject (forbid) feature"""
        self.schema.validation_rules.append(
            ValidationRule(feature_name, "reject", None, reason)
        )
        return self

    def reject_features(
        self,
        feature_names: List[str],
        reason: Optional[str] = None
    ) -> 'FeatureView':
        """Fluent API: reject multiple features"""
        for name in feature_names:
            self.schema.validation_rules.append(
                ValidationRule(name, "reject", None, reason)
            )
        return self


# ============================================================================
# Decorator Functions
# ============================================================================

def _ensure_schema(cls) -> FeatureViewSchema:
    """Ensure class has a feature view schema, raise error if not"""
    if not hasattr(cls, '_feature_view_schema'):
        # Create a temporary schema for collecting rules
        # The @FeatureView decorator will finalize it
        cls._feature_view_schema = FeatureViewSchema(name="", entities=[])
    return cls._feature_view_schema


def _require_feature_view(cls) -> None:
    """Require that class will be decorated with @FeatureView"""
    # We can't enforce this at decorator time since @FeatureView hasn't run yet
    # But we mark it so the test can check
    if not hasattr(cls, '_feature_view_schema'):
        cls._feature_view_schema = FeatureViewSchema(name="", entities=[])
        cls._feature_view_schema._needs_feature_view = True


def assert_type(
    feature_name: str,
    feature_type: Union[type, TypeConstraint],
    reason: Optional[str] = None
) -> Callable:
    """Decorator: Assert feature type

    Example:
        @FeatureView(name="...")
        @assert_type("age", int, "Age must be an integer")
        class UserFeatures:
            pass
    """
    def decorator(cls):
        schema = _ensure_schema(cls)
        constraint = _python_type_to_constraint(feature_type) if isinstance(feature_type, type) else feature_type
        schema.validation_rules.append(
            ValidationRule(feature_name, "assert_type", constraint, reason)
        )
        return cls
    return decorator


def assert_range(
    feature_name: str,
    min: Union[int, float],
    max: Union[int, float],
    reason: Optional[str] = None
) -> Callable:
    """Decorator: Assert value range

    Example:
        @FeatureView(name="...")
        @assert_range("age", min=0, max=150, reason="Age must be realistic")
        class UserFeatures:
            pass
    """
    def decorator(cls):
        schema = _ensure_schema(cls)
        schema.validation_rules.append(
            ValidationRule(feature_name, "assert_range", (min, max), reason)
        )
        return cls
    return decorator


def reject_pii(
    feature_names: List[str],
    reason: str = "PII not allowed in feature store"
) -> Callable:
    """Decorator: Reject PII features

    Example:
        @FeatureView(name="...")
        @reject_pii(["ssn", "credit_card", "email"])
        class UserFeatures:
            pass
    """
    def decorator(cls):
        schema = _ensure_schema(cls)
        for feature_name in feature_names:
            schema.validation_rules.append(
                ValidationRule(feature_name, "reject", None, reason)
            )
        return cls
    return decorator


def reject_feature(
    feature_name: str,
    reason: Optional[str] = None
) -> Callable:
    """Decorator: Reject (forbid) a feature

    Example:
        @FeatureView(name="...")
        @reject_feature("internal_id", "Internal IDs should not be in feature store")
        class UserFeatures:
            pass
    """
    def decorator(cls):
        schema = _ensure_schema(cls)
        schema.validation_rules.append(
            ValidationRule(feature_name, "reject", None, reason)
        )
        return cls
    return decorator


def assert_enum(
    feature_name: str,
    values: List[str],
    reason: Optional[str] = None
) -> Callable:
    """Decorator: Assert string enum

    Example:
        @FeatureView(name="...")
        @assert_enum("status", ["active", "inactive", "suspended"])
        class UserFeatures:
            pass
    """
    def decorator(cls):
        schema = _ensure_schema(cls)
        schema.validation_rules.append(
            ValidationRule(feature_name, "assert_enum", values, reason)
        )
        return cls
    return decorator


def assert_feature(
    feature_name: str,
    reason: Optional[str] = None
) -> Callable:
    """Decorator: Assert feature exists (is required)

    Example:
        @FeatureView(name="...")
        @assert_feature("user_id", "User ID is required")
        class UserFeatures:
            pass
    """
    def decorator(cls):
        schema = _ensure_schema(cls)
        schema.validation_rules.append(
            ValidationRule(feature_name, "assert_feature", None, reason)
        )
        return cls
    return decorator


def assert_length(
    feature_name: str,
    min: int = 0,
    max: int = 2**31,
    reason: Optional[str] = None
) -> Callable:
    """Decorator: Assert string length

    Example:
        @FeatureView(name="...")
        @assert_length("name", min=1, max=100)
        class UserFeatures:
            pass
    """
    def decorator(cls):
        schema = _ensure_schema(cls)
        schema.validation_rules.append(
            ValidationRule(feature_name, "assert_length", (min, max), reason)
        )
        return cls
    return decorator


def assert_pattern(
    feature_name: str,
    pattern: str,
    reason: Optional[str] = None
) -> Callable:
    """Decorator: Assert string matches regex pattern

    NOTE: Pattern is pre-compiled for performance. This means the regex
    is only compiled once at decoration time, not on every validation call.

    Example:
        @FeatureView(name="...")
        @assert_pattern("email", r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$")
        class UserFeatures:
            pass
    """
    # P0-2: Pre-compile pattern for performance
    try:
        compiled_pattern = re.compile(pattern)
    except re.error as e:
        raise ValueError(f"Invalid regex pattern '{pattern}': {e}")

    def decorator(cls):
        schema = _ensure_schema(cls)
        schema.validation_rules.append(
            ValidationRule(feature_name, "assert_pattern", compiled_pattern, reason)
        )
        return cls
    return decorator


def assert_array_length(
    feature_name: str,
    min: int = 0,
    max: int = 2**31,
    reason: Optional[str] = None
) -> Callable:
    """Decorator: Assert array length

    Example:
        @FeatureView(name="...")
        @assert_array_length("tags", min=1, max=10)
        class UserFeatures:
            pass
    """
    def decorator(cls):
        schema = _ensure_schema(cls)
        schema.validation_rules.append(
            ValidationRule(feature_name, "assert_array_length", (min, max), reason)
        )
        return cls
    return decorator


def assert_not_null(
    feature_name: str,
    reason: Optional[str] = None
) -> Callable:
    """Decorator: Assert feature is not null

    Example:
        @FeatureView(name="...")
        @assert_not_null("user_id")
        class UserFeatures:
            pass
    """
    def decorator(cls):
        schema = _ensure_schema(cls)
        schema.validation_rules.append(
            ValidationRule(feature_name, "assert_not_null", None, reason)
        )
        return cls
    return decorator


def assert_custom(
    feature_name: str,
    validator: Callable[[Any], Union[bool, str]],
    reason: Optional[str] = None
) -> Callable:
    """Decorator: Custom validation function

    The validator function receives the feature value and should return:
    - True: validation passed
    - False: validation failed (use `reason` for error message)
    - str: validation failed with custom error message

    Example:
        @FeatureView(name="...")
        @assert_custom("age", lambda x: x >= 18, reason="Must be adult")
        class UserFeatures:
            pass
    """
    def decorator(cls):
        schema = _ensure_schema(cls)
        schema.validation_rules.append(
            ValidationRule(feature_name, "custom", validator, reason)
        )
        return cls
    return decorator


# ============================================================================
# Convenience Functions
# ============================================================================

def validate_feature_row(
    schema: Union[FeatureViewSchema, 'FeatureView'],
    feature_row: dict,
    raise_on_error: bool = False
) -> ValidationResult:
    """Validate a feature row against a schema

    Args:
        schema: FeatureViewSchema or FeatureView to validate against
        feature_row: Dictionary with 'entities' and 'features' keys
        raise_on_error: If True, raise ValidationError on failure

    Returns:
        ValidationResult with is_valid and errors

    Raises:
        ValidationError: If raise_on_error is True and validation fails
    """
    if isinstance(schema, FeatureView):
        schema = schema.schema

    result = schema.validate_detailed(feature_row)

    if not result.is_valid and raise_on_error:
        raise ValidationError(result.errors[0], result.errors)

    return result


def validate_batch(
    schema: Union[FeatureViewSchema, 'FeatureView'],
    feature_rows: List[dict],
    fail_fast: bool = False
) -> Tuple[List[dict], List[Tuple[int, dict, ValidationResult]]]:
    """Validate a batch of feature rows

    Args:
        schema: FeatureViewSchema or FeatureView to validate against
        feature_rows: List of feature row dictionaries
        fail_fast: If True, stop at first invalid row

    Returns:
        Tuple of (valid_rows, invalid_rows)
        invalid_rows contains tuples of (index, row, validation_result)
    """
    if isinstance(schema, FeatureView):
        schema = schema.schema

    valid_rows = []
    invalid_rows = []

    for i, row in enumerate(feature_rows):
        result = schema.validate_detailed(row)
        if result.is_valid:
            valid_rows.append(row)
        else:
            invalid_rows.append((i, row, result))
            if fail_fast:
                break

    return valid_rows, invalid_rows
