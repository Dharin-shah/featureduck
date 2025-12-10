#!/usr/bin/env python3
"""
Research script to evaluate DuckDB's experimental PySpark API

This script tests whether we can use DuckDB's PySpark API as our
transformation abstraction layer instead of building a custom LogicalPlan.

Key Questions:
1. What PySpark features are supported?
2. What's missing?
3. How's the performance vs native DuckDB SQL?
4. Is it stable enough for production?
"""

import sys
import time
import tempfile
from pathlib import Path

try:
    import duckdb
    from duckdb.experimental.spark.sql import SparkSession
    from duckdb.experimental.spark.sql.functions import col, count, avg, sum as sum_, expr
except ImportError as e:
    print(f"ERROR: {e}")
    print("\nPlease install: pip install duckdb")
    sys.exit(1)

print("=" * 80)
print("DuckDB Spark API Research")
print("=" * 80)
print(f"DuckDB Version: {duckdb.__version__}")
print()

# Create test data
temp_dir = tempfile.mkdtemp()
parquet_path = Path(temp_dir) / "test_events.parquet"

print(f"Creating test dataset at: {parquet_path}")
conn = duckdb.connect()
conn.execute("""
    CREATE TABLE events AS 
    SELECT 
        'user_' || (i % 1000)::VARCHAR as user_id,
        'click' as event_type,
        NOW() - INTERVAL (i) HOUR as timestamp,
        (random() * 100)::BIGINT as session_duration
    FROM generate_series(1, 10000) t(i)
""")
conn.execute(f"COPY events TO '{parquet_path}' (FORMAT PARQUET)")
print(f"✓ Created {parquet_path}")
print()

# Test 1: Basic PySpark API usage
print("=" * 80)
print("Test 1: Basic PySpark DataFrame Operations")
print("=" * 80)

try:
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.parquet(str(parquet_path))
    
    print("✓ SparkSession created")
    print("✓ Parquet read successful")
    
    # Test filter
    filtered = df.filter(col("event_type") == "click")
    print(f"✓ Filter works: {filtered.count()} rows")
    
    # Test group by + aggregation
    result = (
        df.filter(col("event_type") == "click")
        .groupBy("user_id")
        .agg(
            count("*").alias("clicks"),
            avg("session_duration").alias("avg_duration")
        )
    )
    print(f"✓ GroupBy + Agg works: {result.count()} groups")
    
    # Show sample
    print("\nSample results:")
    result.show(5)
    
except Exception as e:
    print(f"✗ ERROR: {e}")
    import traceback
    traceback.print_exc()

# Test 2: Time-based filtering (critical for feature engineering)
print()
print("=" * 80)
print("Test 2: Time-based Filtering (Feature Engineering)")
print("=" * 80)

try:
    # Test if DuckDB's Spark API supports INTERVAL expressions
    result = (
        df.filter(col("timestamp") >= expr("NOW() - INTERVAL 7 DAYS"))
        .groupBy("user_id")
        .agg(count("*").alias("clicks_7d"))
    )
    print(f"✓ Time-based filter works: {result.count()} users")
    
except Exception as e:
    print(f"✗ Time-based filter failed: {e}")
    print("  This is CRITICAL for feature engineering!")

# Test 3: Window functions (needed for point-in-time correctness)
print()
print("=" * 80)
print("Test 3: Window Functions (Point-in-Time Correctness)")
print("=" * 80)

try:
    from duckdb.experimental.spark.sql.window import Window
    from duckdb.experimental.spark.sql.functions import row_number
    
    window_spec = Window.partitionBy("user_id").orderBy(col("timestamp").desc())
    result = df.withColumn("row_num", row_number().over(window_spec))
    result_filtered = result.filter(col("row_num") == 1)
    
    print(f"✓ Window functions work: {result_filtered.count()} latest rows")
    
except Exception as e:
    print(f"✗ Window functions failed: {e}")
    print("  This is CRITICAL for point-in-time queries!")

# Test 4: Performance comparison
print()
print("=" * 80)
print("Test 4: Performance (PySpark API vs Native DuckDB SQL)")
print("=" * 80)

# PySpark API
try:
    start = time.time()
    result_spark = (
        df.filter(col("event_type") == "click")
        .groupBy("user_id")
        .agg(count("*").alias("clicks"))
    )
    count_spark = result_spark.count()
    time_spark = time.time() - start
    print(f"PySpark API: {count_spark} rows in {time_spark:.4f}s")
except Exception as e:
    print(f"PySpark API failed: {e}")
    time_spark = None

# Native DuckDB SQL
try:
    start = time.time()
    result_native = conn.execute(f"""
        SELECT user_id, COUNT(*) as clicks
        FROM read_parquet('{parquet_path}')
        WHERE event_type = 'click'
        GROUP BY user_id
    """).fetchall()
    count_native = len(result_native)
    time_native = time.time() - start
    print(f"Native SQL: {count_native} rows in {time_native:.4f}s")
except Exception as e:
    print(f"Native SQL failed: {e}")
    time_native = None

if time_spark and time_native:
    ratio = time_spark / time_native
    print(f"\nRatio: PySpark API is {ratio:.2f}x {'slower' if ratio > 1 else 'faster'} than native SQL")

# Test 5: Feature coverage check
print()
print("=" * 80)
print("Test 5: Feature Coverage (What's Supported?)")
print("=" * 80)

features_to_test = [
    ("filter()", lambda: df.filter(col("user_id") == "user_1")),
    ("groupBy()", lambda: df.groupBy("user_id").count()),
    ("agg()", lambda: df.groupBy("user_id").agg(count("*"))),
    ("join()", lambda: df.join(df, "user_id")),
    ("select()", lambda: df.select("user_id", "event_type")),
    ("withColumn()", lambda: df.withColumn("new_col", col("user_id"))),
    ("orderBy()", lambda: df.orderBy("timestamp")),
    ("limit()", lambda: df.limit(10)),
]

supported = []
unsupported = []

for feature_name, test_func in features_to_test:
    try:
        result = test_func()
        result.count()  # Force execution
        supported.append(feature_name)
        print(f"✓ {feature_name}")
    except Exception as e:
        unsupported.append((feature_name, str(e)))
        print(f"✗ {feature_name}: {e}")

# Summary
print()
print("=" * 80)
print("SUMMARY & RECOMMENDATION")
print("=" * 80)
print(f"\nSupported features: {len(supported)}/{len(features_to_test)}")
print(f"Coverage: {len(supported)/len(features_to_test)*100:.1f}%")

if unsupported:
    print(f"\nUnsupported features:")
    for feature, error in unsupported:
        print(f"  - {feature}: {error[:80]}")

print("\n" + "=" * 80)
print("DECISION FOR M2:")
print("=" * 80)

if len(supported) >= 6 and time_spark and time_native and time_spark/time_native < 2:
    print("✓ RECOMMEND: Use DuckDB's PySpark API")
    print("  - Good feature coverage")
    print("  - Performance acceptable")
    print("  - Less code to maintain")
    print("\nM2 Plan: Provide PySpark API as transformation layer")
else:
    print("⚠ RECOMMEND: Build custom LogicalPlan")
    print("  - Missing critical features OR")
    print("  - Performance overhead too high OR")
    print("  - Too experimental for production")
    print("\nM2 Plan: Build our own transformation abstraction")

print()
print("Cleanup...")
import shutil
shutil.rmtree(temp_dir)
print(f"✓ Removed {temp_dir}")
