"""
Test validation integration with execute_and_write().

Tests the new parameters:
- schema: Optional validation schema dict
- skip_validation: If true, skip validation for faster materialization
"""

import tempfile
import os
import shutil
import pytest
import duckdb


class TestValidationIntegration:
    """Test validation with execute_and_write()."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test data."""
        temp = tempfile.mkdtemp(prefix="validation_test_")
        yield temp
        if os.path.exists(temp):
            shutil.rmtree(temp)

    @pytest.fixture
    def source_data(self, temp_dir):
        """Create source parquet data for testing."""
        source_path = os.path.join(temp_dir, "source")
        os.makedirs(source_path, exist_ok=True)

        conn = duckdb.connect()
        conn.execute(f"""
            COPY (
                SELECT
                    'user_' || row_number() OVER ()::VARCHAR AS user_id,
                    (20 + random() * 50)::INT AS age,
                    (random() * 100)::DOUBLE AS score
                FROM generate_series(1, 10)
            ) TO '{source_path}/data.parquet' (FORMAT PARQUET)
        """)
        conn.close()

        return source_path

    def test_no_validation_default(self, temp_dir, source_data):
        """Test that without schema, no validation is performed."""
        try:
            from featureduck.featureduck_native import execute_and_write
        except ImportError:
            pytest.skip("Rust bindings not installed")

        output_path = os.path.join(temp_dir, "features")

        sql = f"""
            SELECT user_id, SUM(age) as total_age, AVG(score) as avg_score
            FROM read_parquet('{source_data}/*.parquet')
            GROUP BY user_id
        """

        result = execute_and_write(
            sql=sql,
            output_path=output_path,
            feature_view_name="user_features",
            entity_columns=["user_id"],
            mode="overwrite"
        )

        assert result["status"] == "success"
        assert result["rows_written"] > 0

    def test_skip_validation_true(self, temp_dir, source_data):
        """Test that skip_validation=True works."""
        try:
            from featureduck.featureduck_native import execute_and_write
        except ImportError:
            pytest.skip("Rust bindings not installed")

        output_path = os.path.join(temp_dir, "features")

        sql = f"""
            SELECT user_id, SUM(age) as total_age, AVG(score) as avg_score
            FROM read_parquet('{source_data}/*.parquet')
            GROUP BY user_id
        """

        result = execute_and_write(
            sql=sql,
            output_path=output_path,
            feature_view_name="user_features",
            entity_columns=["user_id"],
            mode="overwrite",
            skip_validation=True
        )

        assert result["status"] == "success"
        assert result["rows_written"] > 0

    def test_valid_schema_passes(self, temp_dir, source_data):
        """Test that valid data passes schema validation."""
        try:
            from featureduck.featureduck_native import execute_and_write
        except ImportError:
            pytest.skip("Rust bindings not installed")

        output_path = os.path.join(temp_dir, "features")

        sql = f"""
            SELECT user_id, SUM(age) as total_age, AVG(score) as avg_score
            FROM read_parquet('{source_data}/*.parquet')
            GROUP BY user_id
        """

        # DuckDB SUM returns Float, so use Float type
        schema = {
            "name": "user_features",
            "required_entities": ["user_id"],
            "required_features": [],
            "forbidden_features": [],
            "type_constraints": {
                "total_age": "Float",
                "avg_score": "Float"
            },
            "value_constraints": {}
        }

        result = execute_and_write(
            sql=sql,
            output_path=output_path,
            feature_view_name="user_features",
            entity_columns=["user_id"],
            mode="overwrite",
            schema=schema
        )

        assert result["status"] == "success"
        assert result["rows_written"] > 0

    def test_forbidden_feature_blocked(self, temp_dir, source_data):
        """Test that forbidden features are blocked."""
        try:
            from featureduck.featureduck_native import execute_and_write
        except ImportError:
            pytest.skip("Rust bindings not installed")

        output_path = os.path.join(temp_dir, "features")

        sql = f"""
            SELECT user_id, SUM(age) as total_age, AVG(score) as avg_score
            FROM read_parquet('{source_data}/*.parquet')
            GROUP BY user_id
        """

        # Schema forbids 'avg_score' (simulating PII blocking)
        schema = {
            "name": "user_features",
            "required_entities": ["user_id"],
            "required_features": [],
            "forbidden_features": ["avg_score"],
            "type_constraints": {},
            "value_constraints": {}
        }

        with pytest.raises(RuntimeError) as exc_info:
            execute_and_write(
                sql=sql,
                output_path=output_path,
                feature_view_name="user_features",
                entity_columns=["user_id"],
                mode="overwrite",
                schema=schema
            )

        assert "avg_score" in str(exc_info.value).lower() or "forbidden" in str(exc_info.value).lower()

    def test_skip_validation_bypasses_forbidden(self, temp_dir, source_data):
        """Test that skip_validation=True bypasses forbidden feature check."""
        try:
            from featureduck.featureduck_native import execute_and_write
        except ImportError:
            pytest.skip("Rust bindings not installed")

        output_path = os.path.join(temp_dir, "features")

        sql = f"""
            SELECT user_id, SUM(age) as total_age, AVG(score) as avg_score
            FROM read_parquet('{source_data}/*.parquet')
            GROUP BY user_id
        """

        # Schema forbids 'avg_score', but skip_validation=True
        schema = {
            "name": "user_features",
            "required_entities": ["user_id"],
            "required_features": [],
            "forbidden_features": ["avg_score"],
            "type_constraints": {},
            "value_constraints": {}
        }

        result = execute_and_write(
            sql=sql,
            output_path=output_path,
            feature_view_name="user_features",
            entity_columns=["user_id"],
            mode="overwrite",
            schema=schema,
            skip_validation=True  # Bypass validation
        )

        assert result["status"] == "success"
        assert result["rows_written"] > 0

    def test_type_mismatch_blocked(self, temp_dir, source_data):
        """Test that type mismatches are blocked."""
        try:
            from featureduck.featureduck_native import execute_and_write
        except ImportError:
            pytest.skip("Rust bindings not installed")

        output_path = os.path.join(temp_dir, "features")

        sql = f"""
            SELECT user_id, SUM(age) as total_age, AVG(score) as avg_score
            FROM read_parquet('{source_data}/*.parquet')
            GROUP BY user_id
        """

        # Schema expects 'total_age' to be String, but it's Float
        schema = {
            "name": "user_features",
            "required_entities": ["user_id"],
            "required_features": [],
            "forbidden_features": [],
            "type_constraints": {
                "total_age": "String"  # Wrong type
            },
            "value_constraints": {}
        }

        with pytest.raises(RuntimeError) as exc_info:
            execute_and_write(
                sql=sql,
                output_path=output_path,
                feature_view_name="user_features",
                entity_columns=["user_id"],
                mode="overwrite",
                schema=schema
            )

        assert "type" in str(exc_info.value).lower()

    def test_value_constraint_range(self, temp_dir, source_data):
        """Test that value range constraints work."""
        try:
            from featureduck.featureduck_native import execute_and_write
        except ImportError:
            pytest.skip("Rust bindings not installed")

        output_path = os.path.join(temp_dir, "features")

        sql = f"""
            SELECT user_id, SUM(age) as total_age, AVG(score) as avg_score
            FROM read_parquet('{source_data}/*.parquet')
            GROUP BY user_id
        """

        # Valid range constraint (avg_score is 0-100)
        schema = {
            "name": "user_features",
            "required_entities": ["user_id"],
            "required_features": [],
            "forbidden_features": [],
            "type_constraints": {},
            "value_constraints": {
                "avg_score": [{"type": "FloatRange", "min": 0.0, "max": 100.0}]
            }
        }

        result = execute_and_write(
            sql=sql,
            output_path=output_path,
            feature_view_name="user_features",
            entity_columns=["user_id"],
            mode="overwrite",
            schema=schema
        )

        assert result["status"] == "success"
