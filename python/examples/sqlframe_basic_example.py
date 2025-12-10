"""
SQLFrame Basic Example - User Purchase Features

This example demonstrates the new SQLFrame-based API with PySpark compatibility.

What this example shows:
1. Creating a SQLFrameFeatureView with DuckDB backend
2. Reading source data (using in-memory data for demo)
3. Applying PySpark transformations (filter, groupBy, agg)
4. Extracting SQL for inspection
5. Materializing features (stubbed - will integrate with Rust core in Phase 1)

Benefits over custom DataFrame API:
- ✅ PySpark API (99% of users already know it)
- ✅ Window functions supported
- ✅ Engine switching (DuckDB ↔ Spark in 1 line)
- ✅ Future-proof (7+ backends via SQLFrame)
"""

from featureduck import SQLFrameFeatureView, F

# ============================================================================
# Example 1: Basic Aggregations
# ============================================================================

def example_basic_aggregations():
    """Basic aggregations with filter and groupBy."""
    print("=" * 80)
    print("Example 1: Basic Aggregations")
    print("=" * 80)

    # Create feature view with DuckDB backend
    view = SQLFrameFeatureView(
        name="user_purchases",
        entities=["user_id"],
        engine="duckdb"
    )

    # Create sample data
    data = [
        ("user_1", "purchase", 100.0, "2024-01-01"),
        ("user_1", "purchase", 50.0, "2024-01-02"),
        ("user_2", "purchase", 200.0, "2024-01-01"),
        ("user_1", "view", 0.0, "2024-01-03"),
        ("user_2", "purchase", 150.0, "2024-01-02"),
    ]

    df = view.session.createDataFrame(
        data,
        ["user_id", "event_type", "amount", "timestamp"]
    )

    # Apply PySpark transformations
    features = (df
        .filter(F.col("event_type") == "purchase")
        .groupBy("user_id")
        .agg(
            F.count("*").alias("purchase_count"),
            F.sum("amount").alias("total_revenue"),
            F.avg("amount").alias("avg_order_value")
        )
    )

    # Show results
    print("\nTransformation:")
    print("  Filter: event_type == 'purchase'")
    print("  GroupBy: user_id")
    print("  Aggregations: count, sum(amount), avg(amount)")
    print("\nResults:")
    features.show()

    # Get generated SQL
    sql = view.get_sql(features)
    print("\nGenerated SQL:")
    print(sql)
    print()


# ============================================================================
# Example 2: Window Functions (NOT possible with custom DataFrame API!)
# ============================================================================

def example_window_functions():
    """Window functions - running totals and rankings."""
    print("=" * 80)
    print("Example 2: Window Functions (NEW capability!)")
    print("=" * 80)

    view = SQLFrameFeatureView(
        name="user_revenue",
        entities=["user_id"],
        engine="duckdb"
    )

    # Sample data: daily revenue per user
    data = [
        ("user_1", 100.0, "2024-01-01"),
        ("user_1", 150.0, "2024-01-02"),
        ("user_1", 200.0, "2024-01-03"),
        ("user_2", 50.0, "2024-01-01"),
        ("user_2", 75.0, "2024-01-02"),
    ]

    df = view.session.createDataFrame(data, ["user_id", "revenue", "date"])

    # Window specification
    from pyspark.sql import Window
    window_spec = Window.partitionBy("user_id").orderBy("date")

    # Apply window functions
    features = df.select(
        "user_id",
        "date",
        "revenue",
        F.sum("revenue").over(window_spec).alias("running_total"),
        F.row_number().over(window_spec).alias("day_number"),
        F.lag("revenue", 1).over(window_spec).alias("prev_day_revenue")
    )

    print("\nTransformation:")
    print("  Window: PARTITION BY user_id ORDER BY date")
    print("  Functions: SUM (running total), ROW_NUMBER, LAG")
    print("\nResults:")
    features.show()

    sql = view.get_sql(features)
    print("\nGenerated SQL:")
    print(sql[:500] + "..." if len(sql) > 500 else sql)
    print()


# ============================================================================
# Example 3: Complex Multi-Step Transformation
# ============================================================================

def example_complex_transformation():
    """Complex transformation with conditional aggregations."""
    print("=" * 80)
    print("Example 3: Complex Multi-Step Transformation")
    print("=" * 80)

    view = SQLFrameFeatureView(
        name="user_funnel",
        entities=["user_id"],
        engine="duckdb"
    )

    # E-commerce funnel data
    data = [
        ("user_1", "view", "product_A", "2024-01-01 10:00:00"),
        ("user_1", "cart", "product_A", "2024-01-01 10:05:00"),
        ("user_1", "purchase", "product_A", "2024-01-01 10:10:00"),
        ("user_2", "view", "product_B", "2024-01-01 11:00:00"),
        ("user_2", "view", "product_C", "2024-01-01 11:05:00"),
        ("user_2", "cart", "product_B", "2024-01-01 11:10:00"),
        ("user_1", "view", "product_B", "2024-01-02 09:00:00"),
    ]

    df = view.session.createDataFrame(
        data,
        ["user_id", "event_type", "product_id", "timestamp"]
    )

    # Multi-step transformation
    features = (df
        .groupBy("user_id")
        .agg(
            F.count("*").alias("total_events"),
            F.count(F.when(F.col("event_type") == "view", 1)).alias("view_count"),
            F.count(F.when(F.col("event_type") == "cart", 1)).alias("cart_count"),
            F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
            F.countDistinct("product_id").alias("unique_products")
        )
        .withColumn(
            "view_to_purchase_rate",
            F.col("purchase_count") / F.col("view_count")
        )
        .withColumn(
            "is_high_intent",
            F.when(F.col("cart_count") > 0, True).otherwise(False)
        )
    )

    print("\nTransformation:")
    print("  Conditional counts: COUNT(CASE WHEN ...)")
    print("  Distinct count: COUNT(DISTINCT product_id)")
    print("  Derived columns: conversion rate, high intent flag")
    print("\nResults:")
    features.show()

    sql = view.get_sql(features)
    print("\nGenerated SQL (truncated):")
    print(sql[:600] + "..." if len(sql) > 600 else sql)
    print()


# ============================================================================
# Example 4: Engine Switching (DuckDB ↔ Spark)
# ============================================================================

def example_engine_switching():
    """Same code, different engines."""
    print("=" * 80)
    print("Example 4: Engine Switching (DuckDB ↔ Spark)")
    print("=" * 80)

    # DuckDB version (local development)
    print("\nDuckDB Backend (local development):")
    view_duckdb = SQLFrameFeatureView(
        name="user_features",
        entities=["user_id"],
        engine="duckdb"
    )
    print(f"  Engine: {view_duckdb.engine}")
    print(f"  Session: {type(view_duckdb.session).__name__}")

    # Spark version (production) - same code!
    print("\nSpark Backend (production) - SAME CODE:")
    try:
        view_spark = SQLFrameFeatureView(
            name="user_features",
            entities=["user_id"],
            engine="spark"
        )
        print(f"  Engine: {view_spark.engine}")
        print(f"  Session: {type(view_spark.session).__name__}")
        print("  ✅ Spark session created successfully!")
    except Exception as e:
        print(f"  ⚠️  Spark not available: {e}")
        print("  (This is expected if Spark is not installed)")

    print("\nKey Point:")
    print("  Same transformation code runs on both engines.")
    print("  Switch engines by changing ONE line: engine='duckdb' → engine='spark'")
    print()


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("SQLFrame API Examples - FeatureDuck")
    print("=" * 80)
    print()
    print("This demonstrates the new PySpark-compatible API using SQLFrame.")
    print("Benefits: Zero learning curve, window functions, engine switching.")
    print()

    try:
        # Example 1: Basic aggregations
        example_basic_aggregations()

        # Example 2: Window functions (NEW!)
        example_window_functions()

        # Example 3: Complex transformations
        example_complex_transformation()

        # Example 4: Engine switching
        example_engine_switching()

        print("=" * 80)
        print("✅ All examples completed successfully!")
        print("=" * 80)
        print()
        print("Next steps:")
        print("  1. Try modifying these examples")
        print("  2. Read the SQLFrame documentation: github.com/eakmanrq/sqlframe")
        print("  3. Migrate your existing code from custom DataFrame API")
        print()

    except ImportError as e:
        print()
        print("=" * 80)
        print("⚠️  SQLFrame not installed")
        print("=" * 80)
        print()
        print(f"Error: {e}")
        print()
        print("Installation:")
        print("  pip install 'sqlframe[duckdb]'")
        print()
        print("Or use virtual environment:")
        print("  python3 -m venv .venv-sqlframe")
        print("  source .venv-sqlframe/bin/activate")
        print("  pip install 'sqlframe[duckdb]'")
        print()
