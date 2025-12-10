#!/usr/bin/env python3
"""
Full End-to-End Pipeline: Define → Register → Materialize → Read

This demonstrates the complete FeatureDuck workflow using the Tecton-style
decorator API with SQLFrame for PySpark-compatible transformations.

Run this example:
    cd python
    source .venv/bin/activate
    python examples/e2e_full_pipeline.py
"""

import os
import sys
import json
import tempfile
import shutil
import sqlite3
import duckdb
from datetime import datetime

from sqlframe.duckdb import DuckDBSession
from sqlframe.duckdb import functions as F

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from featureduck.feature_definition import FeatureDefinition, Entity, DataSource


# ==============================================================================
# STEP 1: Setup Test Environment
# ==============================================================================
def setup_test_environment():
    """Create temporary directories and test data."""
    # Create temp directories
    temp_dir = tempfile.mkdtemp(prefix="featureduck_e2e_")
    source_path = os.path.join(temp_dir, "source_data")
    output_path = os.path.join(temp_dir, "features")
    registry_path = os.path.join(temp_dir, "registry.db")

    os.makedirs(source_path, exist_ok=True)
    os.makedirs(output_path, exist_ok=True)

    print(f"\n{'='*80}")
    print(" STEP 1: Setup Test Environment")
    print(f"{'='*80}")
    print(f"Temp directory: {temp_dir}")
    print(f"Source data:    {source_path}")
    print(f"Feature output: {output_path}")
    print(f"Registry:       {registry_path}")

    # Create source parquet data with DuckDB
    conn = duckdb.connect()
    conn.execute(f"""
        COPY (
            SELECT
                'user_' || (row_number() OVER ())::VARCHAR AS user_id,
                'product_' || (1 + (row_number() OVER ()) % 50)::VARCHAR AS product_id,
                CASE WHEN random() < 0.6 THEN 'purchase'
                     WHEN random() < 0.8 THEN 'view'
                     ELSE 'add_to_cart' END AS event_type,
                (50 + random() * 450)::DOUBLE AS amount,
                TIMESTAMP '2024-01-01' + INTERVAL (random() * 30) DAY AS event_time,
                'completed' AS status
            FROM generate_series(1, 500)
        ) TO '{source_path}/events.parquet' (FORMAT PARQUET)
    """)
    conn.close()

    # Verify source data
    verify_conn = duckdb.connect()
    result = verify_conn.execute(f"SELECT COUNT(*) FROM read_parquet('{source_path}/*.parquet')").fetchone()
    print(f"Source data created: {result[0]} rows")
    verify_conn.close()

    return temp_dir, source_path, output_path, registry_path


# ==============================================================================
# STEP 2: Define Features using Tecton-style Decorator API
# ==============================================================================
def define_features(source_path):
    """Define feature views using Tecton-style decorator API with SQLFrame."""
    print(f"\n{'='*80}")
    print(" STEP 2: Define Features (Tecton-style + SQLFrame)")
    print(f"{'='*80}")

    # Define entity
    user = Entity(
        name="user",
        join_keys=["user_id"],
        description="User entity for ML features"
    )

    # Define data source
    events = DataSource(
        name="events",
        source_type="parquet",
        path=source_path,
        timestamp_field="event_time",
        description="User interaction events"
    )

    # Define feature view with transformation
    @FeatureDefinition(
        name="user_purchase_features",
        entities=[user],
        source=events,
        ttl_days=30,
        batch_schedule="0 2 * * *",  # Daily at 2am
        description="User purchase behavior features",
        owner="data-team@example.com"
    )
    def user_purchases(df):
        """
        PySpark-style transformation that SQLFrame converts to DuckDB SQL.
        """
        return (df
            .filter(F.col("event_type") == "purchase")
            .filter(F.col("status") == "completed")
            .groupBy("user_id")
            .agg(
                F.count("*").alias("purchase_count"),
                F.sum("amount").alias("total_spent"),
                F.avg("amount").alias("avg_order_value"),
                F.countDistinct("product_id").alias("unique_products"),
                F.min("event_time").alias("first_purchase"),
                F.max("event_time").alias("last_purchase"),
            )
        )

    # Show generated SQL
    sql = user_purchases.to_sql()
    print(f"\nFeature View: {user_purchases.name}")
    print(f"Entities: {[e.name for e in user_purchases.entities]}")
    print(f"TTL: {user_purchases.ttl_days} days")
    print(f"\nGenerated SQL:\n{sql}")

    return user_purchases


# ==============================================================================
# STEP 3: Register Feature View to Registry
# ==============================================================================
def register_features(feature_view, registry_path):
    """Apply feature view to the registry database."""
    print(f"\n{'='*80}")
    print(" STEP 3: Register Feature View to Registry")
    print(f"{'='*80}")

    # Apply to registry
    success = feature_view.apply(registry_path)

    if success:
        # Verify registration
        conn = sqlite3.connect(registry_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name, version, source_type, source_path, entities FROM feature_views WHERE name = ?",
                      (feature_view.name,))
        row = cursor.fetchone()
        conn.close()

        if row:
            print(f"\nRegistered feature view:")
            print(f"  Name: {row[0]}")
            print(f"  Version: {row[1]}")
            print(f"  Source Type: {row[2]}")
            print(f"  Source Path: {row[3]}")
            print(f"  Entities: {row[4]}")
            return True

    print("Failed to register feature view!")
    return False


# ==============================================================================
# STEP 4: Materialize Features (Execute SQL → Write to Output)
# ==============================================================================
def materialize_features(feature_view, output_path):
    """
    Execute the SQL transformation and write results.
    This simulates what the CLI 'materialize' command does.
    """
    print(f"\n{'='*80}")
    print(" STEP 4: Materialize Features (Execute SQL)")
    print(f"{'='*80}")

    # Get the SQL - the feature_definition module now generates clean DuckDB SQL
    sql = feature_view.to_sql()

    print(f"Executing SQL against source data...")
    print(f"\nExecuting SQL:\n{sql}\n")

    # Execute with DuckDB
    conn = duckdb.connect()

    # Execute the query and get results
    result = conn.execute(sql)

    # Write to parquet using DuckDB's COPY (no pyarrow needed)
    feature_output_path = os.path.join(output_path, feature_view.name)
    os.makedirs(feature_output_path, exist_ok=True)
    parquet_path = os.path.join(feature_output_path, "features.parquet")

    # Use COPY with the query result
    conn.execute(f"""
        COPY ({sql}) TO '{parquet_path}' (FORMAT PARQUET)
    """)

    # Get row count
    row_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')").fetchone()[0]

    # Show sample results
    sample_df = conn.execute(f"SELECT * FROM read_parquet('{parquet_path}') LIMIT 5").fetchdf()

    print(f"Query returned {row_count} rows")
    print(f"\nSample results (first 5 rows):")
    print(sample_df)

    print(f"\nMaterialized features written to: {parquet_path}")

    conn.close()
    return row_count, feature_output_path


# ==============================================================================
# STEP 5: Read Features Back (Point-in-Time Query)
# ==============================================================================
def read_features(feature_path, entity_ids=None):
    """
    Read features back from the materialized output.
    This simulates what the feature serving API does.
    """
    print(f"\n{'='*80}")
    print(" STEP 5: Read Features (Point-in-Time Query)")
    print(f"{'='*80}")

    conn = duckdb.connect()

    # Build query
    if entity_ids:
        entity_filter = ", ".join(f"'{eid}'" for eid in entity_ids)
        sql = f"""
            SELECT *
            FROM read_parquet('{feature_path}/*.parquet')
            WHERE user_id IN ({entity_filter})
            ORDER BY user_id
        """
    else:
        sql = f"""
            SELECT *
            FROM read_parquet('{feature_path}/*.parquet')
            ORDER BY total_spent DESC
            LIMIT 10
        """

    print(f"Reading features for: {entity_ids if entity_ids else 'top 10 by total_spent'}")

    result = conn.execute(sql).fetchdf()
    print(f"\nFeature values:")
    print(result)

    conn.close()
    return result


# ==============================================================================
# STEP 6: Verify End-to-End
# ==============================================================================
def verify_e2e(source_path, features_df, row_count):
    """Verify the end-to-end pipeline worked correctly."""
    print(f"\n{'='*80}")
    print(" STEP 6: Verify End-to-End Pipeline")
    print(f"{'='*80}")

    checks_passed = 0
    total_checks = 5

    # Check 1: Features were generated
    if row_count > 0:
        print(f"[PASS] Features generated: {row_count} rows")
        checks_passed += 1
    else:
        print(f"[FAIL] No features generated")

    # Check 2: Required columns exist
    required_cols = ['user_id', 'purchase_count', 'total_spent', 'avg_order_value']
    if all(col in features_df.columns for col in required_cols):
        print(f"[PASS] Required columns present: {required_cols}")
        checks_passed += 1
    else:
        missing = [c for c in required_cols if c not in features_df.columns]
        print(f"[FAIL] Missing columns: {missing}")

    # Check 3: Values are reasonable
    if features_df['purchase_count'].min() > 0:
        print(f"[PASS] All users have at least 1 purchase")
        checks_passed += 1
    else:
        print(f"[FAIL] Found users with 0 purchases (shouldn't happen with filter)")

    # Check 4: Total spent is positive
    if features_df['total_spent'].min() > 0:
        print(f"[PASS] Total spent is positive for all users")
        checks_passed += 1
    else:
        print(f"[FAIL] Found negative or zero total_spent")

    # Check 5: Average calculation is correct (spot check)
    # total_spent / purchase_count should approximately equal avg_order_value
    features_df['calculated_avg'] = features_df['total_spent'] / features_df['purchase_count']
    diff = (features_df['avg_order_value'] - features_df['calculated_avg']).abs().max()
    if diff < 0.01:
        print(f"[PASS] Average order value calculation verified")
        checks_passed += 1
    else:
        print(f"[FAIL] Average order value mismatch (max diff: {diff})")

    print(f"\n{'='*80}")
    print(f" RESULTS: {checks_passed}/{total_checks} checks passed")
    print(f"{'='*80}")

    return checks_passed == total_checks


# ==============================================================================
# MAIN: Run Full E2E Pipeline
# ==============================================================================
def main():
    print("\n" + "="*80)
    print(" FEATUREDUCK: Full End-to-End Pipeline Test")
    print(" Define → Register → Materialize → Read → Verify")
    print("="*80)

    temp_dir = None
    try:
        # Step 1: Setup
        temp_dir, source_path, output_path, registry_path = setup_test_environment()

        # Step 2: Define
        feature_view = define_features(source_path)

        # Step 3: Register
        if not register_features(feature_view, registry_path):
            print("\nE2E FAILED: Could not register feature view")
            return 1

        # Step 4: Materialize
        row_count, feature_path = materialize_features(feature_view, output_path)

        # Step 5: Read
        features_df = read_features(feature_path)

        # Step 6: Verify
        if verify_e2e(source_path, features_df, row_count):
            print("\n" + "="*80)
            print(" E2E PIPELINE: SUCCESS!")
            print("="*80 + "\n")
            return 0
        else:
            print("\n" + "="*80)
            print(" E2E PIPELINE: SOME CHECKS FAILED")
            print("="*80 + "\n")
            return 1

    except Exception as e:
        print(f"\n[ERROR] E2E pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        # Cleanup
        if temp_dir and os.path.exists(temp_dir):
            print(f"\nCleaning up temp directory: {temp_dir}")
            shutil.rmtree(temp_dir)


if __name__ == "__main__":
    sys.exit(main())
