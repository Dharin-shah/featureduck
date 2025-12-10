"""
End-to-End Tests: Python → Rust → Delta Lake

Tests the complete cycle:
1. Python generates SQL via SQLFrame
2. Rust executes SQL and writes to Delta Lake
3. Read back features from Delta Lake

Run with:
    PYTHONPATH=python python3.11 -m pytest python/tests/test_e2e_python_rust.py -v
"""

import os
import tempfile
import shutil
import pytest
import duckdb

# NOTE: Do not add python/ to sys.path - use the installed wheel instead
# sys.path manipulation would override the wheel's Rust bindings


class TestPythonRustE2E:
    """End-to-end tests for Python → Rust → Delta Lake pipeline."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test data."""
        temp = tempfile.mkdtemp(prefix="featureduck_e2e_")
        yield temp
        # Cleanup after test
        if os.path.exists(temp):
            shutil.rmtree(temp)

    @pytest.fixture
    def source_data(self, temp_dir):
        """Create source parquet data for testing."""
        source_path = os.path.join(temp_dir, "source")
        os.makedirs(source_path, exist_ok=True)

        # Create test data with DuckDB
        conn = duckdb.connect()
        conn.execute(f"""
            COPY (
                SELECT
                    'user_' || row_number() OVER ()::VARCHAR AS user_id,
                    CASE WHEN random() < 0.7 THEN 'purchase' ELSE 'view' END AS event_type,
                    (50 + random() * 450)::DOUBLE AS amount,
                    TIMESTAMP '2024-01-01' + INTERVAL (random() * 30) DAY AS event_time
                FROM generate_series(1, 100)
            ) TO '{source_path}/events.parquet' (FORMAT PARQUET)
        """)
        conn.close()

        return source_path

    def test_rust_bridge_import(self):
        """Test that the Rust native module can be imported."""
        try:
            from featureduck.featureduck_native import execute_and_write
            assert callable(execute_and_write)
        except ImportError as e:
            pytest.skip(f"Rust bindings not installed: {e}")

    def test_execute_and_write_basic(self, temp_dir, source_data):
        """Test basic execute_and_write functionality."""
        try:
            from featureduck.featureduck_native import execute_and_write
        except ImportError:
            pytest.skip("Rust bindings not installed")

        output_path = os.path.join(temp_dir, "features")

        # Generate SQL (simulating SQLFrame output)
        sql = f"""
            SELECT
                user_id,
                COUNT(*) as purchase_count,
                SUM(amount) as total_spent,
                AVG(amount) as avg_order_value
            FROM read_parquet('{source_data}/*.parquet')
            WHERE event_type = 'purchase'
            GROUP BY user_id
        """

        # Execute via Rust bridge
        result = execute_and_write(
            sql=sql,
            output_path=output_path,
            feature_view_name="user_features",
            entity_columns=["user_id"],
            mode="overwrite"
        )

        # Verify result
        assert result["status"] == "success"
        assert int(result["rows_written"]) > 0
        assert result["output_path"] == output_path

    def test_full_cycle_write_and_read(self, temp_dir, source_data):
        """Test complete cycle: Python SQL → Rust Write → DuckDB Read."""
        try:
            from featureduck.featureduck_native import execute_and_write
        except ImportError:
            pytest.skip("Rust bindings not installed")

        output_path = os.path.join(temp_dir, "features")
        feature_view = "user_purchase_features"

        # Step 1: Generate SQL
        sql = f"""
            SELECT
                user_id,
                COUNT(*) as purchase_count,
                SUM(amount) as total_spent
            FROM read_parquet('{source_data}/*.parquet')
            WHERE event_type = 'purchase'
            GROUP BY user_id
        """

        # Step 2: Execute via Rust (writes to Delta Lake)
        result = execute_and_write(
            sql=sql,
            output_path=output_path,
            feature_view_name=feature_view,
            entity_columns=["user_id"],
            mode="overwrite"
        )

        assert result["status"] == "success"
        rows_written = int(result["rows_written"])
        assert rows_written > 0

        # Step 3: Read back from Delta Lake using DuckDB
        conn = duckdb.connect()
        conn.execute("INSTALL delta; LOAD delta;")

        delta_path = os.path.join(output_path, feature_view)
        read_sql = f"SELECT * FROM delta_scan('{delta_path}')"

        df = conn.execute(read_sql).fetchdf()
        conn.close()

        # Step 4: Verify
        assert len(df) == rows_written
        assert "user_id" in df.columns
        assert "purchase_count" in df.columns
        assert "total_spent" in df.columns
        assert "__timestamp" in df.columns  # Delta Lake timestamp

    def test_append_mode(self, temp_dir, source_data):
        """Test append mode writes."""
        try:
            from featureduck.featureduck_native import execute_and_write
        except ImportError:
            pytest.skip("Rust bindings not installed")

        output_path = os.path.join(temp_dir, "features")
        feature_view = "append_test"

        sql = f"""
            SELECT
                user_id,
                COUNT(*) as count
            FROM read_parquet('{source_data}/*.parquet')
            WHERE user_id IN ('user_1', 'user_2')
            GROUP BY user_id
        """

        # First write
        result1 = execute_and_write(
            sql=sql,
            output_path=output_path,
            feature_view_name=feature_view,
            entity_columns=["user_id"],
            mode="overwrite"
        )
        rows1 = int(result1["rows_written"])

        # Second write (append)
        result2 = execute_and_write(
            sql=sql,
            output_path=output_path,
            feature_view_name=feature_view,
            entity_columns=["user_id"],
            mode="append"
        )
        rows2 = int(result2["rows_written"])

        # Verify both rows are present
        conn = duckdb.connect()
        conn.execute("INSTALL delta; LOAD delta;")

        delta_path = os.path.join(output_path, feature_view)
        count = conn.execute(f"SELECT COUNT(*) FROM delta_scan('{delta_path}')").fetchone()[0]
        conn.close()

        # Total should be sum of both writes
        assert count == rows1 + rows2

    def test_sqlframe_integration(self, temp_dir, source_data):
        """Test SQLFrame → Rust bridge integration."""
        try:
            from featureduck.featureduck_native import execute_and_write
            from sqlframe.duckdb import DuckDBSession
            from sqlframe.duckdb import functions as F
        except ImportError as e:
            pytest.skip(f"Required module not installed: {e}")

        output_path = os.path.join(temp_dir, "features")
        feature_view = "sqlframe_features"

        # Step 1: Use SQLFrame to generate SQL
        # Note: We create a VIEW for SQLFrame to work with, but the generated SQL
        # references "events" which Rust's DuckDB won't know about. We'll fix this
        # by replacing the table reference with read_parquet() after SQL generation.
        conn = duckdb.connect()
        conn.execute(f"CREATE VIEW events AS SELECT * FROM read_parquet('{source_data}/*.parquet')")

        session = DuckDBSession.builder.config("conn", conn).getOrCreate()

        df = session.read.table("events")
        result_df = (df
            .filter(F.col("event_type") == "purchase")
            .groupBy("user_id")
            .agg(
                F.count("*").alias("purchase_count"),
                F.sum("amount").alias("total_spent")
            )
        )

        # Get SQL from SQLFrame (transpiled to DuckDB dialect)
        sql = result_df.expression.sql(dialect="duckdb")
        conn.close()

        # Replace the VIEW reference with read_parquet() so Rust's DuckDB can execute it
        # SQLFrame generates SQL like "FROM events" but Rust has no VIEW "events"
        sql = sql.replace('"events"', f"read_parquet('{source_data}/*.parquet')")
        sql = sql.replace("FROM events", f"FROM read_parquet('{source_data}/*.parquet')")

        # Step 2: Execute via Rust bridge
        result = execute_and_write(
            sql=sql,
            output_path=output_path,
            feature_view_name=feature_view,
            entity_columns=["user_id"],
            mode="overwrite"
        )

        assert result["status"] == "success"
        assert int(result["rows_written"]) > 0

    def test_multiple_entity_columns(self, temp_dir):
        """Test with multiple entity columns."""
        try:
            from featureduck.featureduck_native import execute_and_write
        except ImportError:
            pytest.skip("Rust bindings not installed")

        # Create source with multiple entity columns
        source_path = os.path.join(temp_dir, "source")
        os.makedirs(source_path, exist_ok=True)

        conn = duckdb.connect()
        conn.execute(f"""
            COPY (
                SELECT
                    'user_' || ((row_number() OVER ()) % 10)::VARCHAR AS user_id,
                    'product_' || ((row_number() OVER ()) % 5)::VARCHAR AS product_id,
                    (random() * 100)::DOUBLE AS score
                FROM generate_series(1, 50)
            ) TO '{source_path}/data.parquet' (FORMAT PARQUET)
        """)
        conn.close()

        output_path = os.path.join(temp_dir, "features")

        sql = f"""
            SELECT
                user_id,
                product_id,
                AVG(score) as avg_score,
                COUNT(*) as interaction_count
            FROM read_parquet('{source_path}/*.parquet')
            GROUP BY user_id, product_id
        """

        result = execute_and_write(
            sql=sql,
            output_path=output_path,
            feature_view_name="user_product_features",
            entity_columns=["user_id", "product_id"],
            mode="overwrite"
        )

        assert result["status"] == "success"
        rows = int(result["rows_written"])
        assert rows > 0
        assert rows <= 50  # At most 10 users * 5 products

    def test_error_handling_invalid_sql(self, temp_dir):
        """Test error handling for invalid SQL."""
        try:
            from featureduck.featureduck_native import execute_and_write
        except ImportError:
            pytest.skip("Rust bindings not installed")

        output_path = os.path.join(temp_dir, "features")

        # Invalid SQL
        sql = "SELECT * FROM nonexistent_table_xyz"

        with pytest.raises(RuntimeError) as exc_info:
            execute_and_write(
                sql=sql,
                output_path=output_path,
                feature_view_name="test",
                entity_columns=["id"],
                mode="overwrite"
            )

        assert "Failed" in str(exc_info.value) or "error" in str(exc_info.value).lower()

    def test_error_handling_invalid_mode(self, temp_dir, source_data):
        """Test error handling for invalid write mode."""
        try:
            from featureduck.featureduck_native import execute_and_write
        except ImportError:
            pytest.skip("Rust bindings not installed")

        output_path = os.path.join(temp_dir, "features")

        sql = f"SELECT user_id, COUNT(*) as cnt FROM read_parquet('{source_data}/*.parquet') GROUP BY user_id"

        with pytest.raises(ValueError) as exc_info:
            execute_and_write(
                sql=sql,
                output_path=output_path,
                feature_view_name="test",
                entity_columns=["user_id"],
                mode="invalid_mode"
            )

        assert "Unsupported write mode" in str(exc_info.value)


class TestE2EPerformance:
    """Performance-focused E2E tests."""

    @pytest.fixture
    def large_source_data(self, tmp_path):
        """Create larger source data for performance testing."""
        source_path = str(tmp_path / "source")
        os.makedirs(source_path, exist_ok=True)

        conn = duckdb.connect()
        conn.execute(f"""
            COPY (
                SELECT
                    'user_' || ((row_number() OVER ()) % 1000)::VARCHAR AS user_id,
                    CASE WHEN random() < 0.7 THEN 'purchase' ELSE 'view' END AS event_type,
                    (50 + random() * 450)::DOUBLE AS amount
                FROM generate_series(1, 10000)
            ) TO '{source_path}/events.parquet' (FORMAT PARQUET)
        """)
        conn.close()

        return source_path

    def test_performance_10k_rows(self, tmp_path, large_source_data):
        """Test performance with 10K source rows."""
        try:
            from featureduck.featureduck_native import execute_and_write
        except ImportError:
            pytest.skip("Rust bindings not installed")

        import time

        output_path = str(tmp_path / "features")

        sql = f"""
            SELECT
                user_id,
                COUNT(*) as purchase_count,
                SUM(amount) as total_spent,
                AVG(amount) as avg_order_value,
                MIN(amount) as min_order,
                MAX(amount) as max_order
            FROM read_parquet('{large_source_data}/*.parquet')
            WHERE event_type = 'purchase'
            GROUP BY user_id
        """

        start = time.time()
        result = execute_and_write(
            sql=sql,
            output_path=output_path,
            feature_view_name="perf_test",
            entity_columns=["user_id"],
            mode="overwrite"
        )
        elapsed = time.time() - start

        assert result["status"] == "success"
        rows = int(result["rows_written"])

        # Performance assertions
        assert elapsed < 10.0, f"Took too long: {elapsed:.2f}s"
        assert rows > 0

        print(f"\nPerformance: {rows} rows in {elapsed:.3f}s ({rows/elapsed:.0f} rows/sec)")
