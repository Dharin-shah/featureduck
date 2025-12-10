"""
Comprehensive SQL Generation Examples using SQLFrame + PySpark API.

This demonstrates how FeatureDuck converts PySpark transformations to optimized
DuckDB SQL for efficient feature computation. SQLFrame handles the conversion
from PySpark DataFrame API to SQL, generating optimal queries.

Run this file to see all SQL patterns:
    cd python
    source .venv/bin/activate  # or: .venv/bin/python
    python examples/comprehensive_sql_examples.py
"""

import duckdb
from sqlframe.duckdb import DuckDBSession
from sqlframe.duckdb import functions as F
from sqlframe.duckdb import Window


def setup_session():
    """Create SQLFrame session with mock tables for demonstration."""
    # Create DuckDB connection first (more reliable for DDL)
    conn = duckdb.connect()

    # Create comprehensive event table
    conn.execute("""
        CREATE TABLE events AS
        SELECT
            'user_' || (row_number() OVER ())::VARCHAR AS user_id,
            'product_' || (1 + (row_number() OVER ()) % 100)::VARCHAR AS product_id,
            CASE WHEN random() < 0.6 THEN 'purchase'
                 WHEN random() < 0.8 THEN 'view'
                 ELSE 'add_to_cart' END AS event_type,
            (100 + random() * 900)::DOUBLE AS amount,
            TIMESTAMP '2024-01-01' + INTERVAL (random() * 365) DAY AS event_time,
            'session_' || (random() * 1000)::INT::VARCHAR AS session_id,
            CASE WHEN random() < 0.9 THEN 'completed' ELSE 'pending' END AS status
        FROM generate_series(1, 1000)
    """)

    # Create user profile table
    conn.execute("""
        CREATE TABLE users AS
        SELECT
            'user_' || (row_number() OVER ())::VARCHAR AS user_id,
            CASE WHEN random() < 0.5 THEN 'premium' ELSE 'standard' END AS tier,
            DATE '2020-01-01' + INTERVAL (random() * 1500) DAY AS signup_date,
            CASE WHEN random() < 0.4 THEN 'US'
                 WHEN random() < 0.7 THEN 'UK'
                 ELSE 'EU' END AS country
        FROM generate_series(1, 200)
    """)

    # Create products table
    conn.execute("""
        CREATE TABLE products AS
        SELECT
            'product_' || (row_number() OVER ())::VARCHAR AS product_id,
            CASE WHEN random() < 0.3 THEN 'electronics'
                 WHEN random() < 0.6 THEN 'clothing'
                 ELSE 'home' END AS category,
            (10 + random() * 990)::DOUBLE AS price,
            (random() * 5)::DOUBLE AS avg_rating
        FROM generate_series(1, 100)
    """)

    # Pass connection to SQLFrame
    session = DuckDBSession.builder.config("conn", conn).getOrCreate()
    return session


def print_sql(name, df):
    """Print optimized SQL from DataFrame."""
    print(f"\n{'='*80}")
    print(f" {name}")
    print(f"{'='*80}")
    sql = df.sql(optimize=True)
    print(sql)
    print()


# ==============================================================================
# EXAMPLE 1: Basic Aggregations
# ==============================================================================
def example_basic_aggregations(session):
    """Basic COUNT, SUM, AVG, MIN, MAX aggregations."""
    events = session.read.table("events")

    result = (events
        .filter(F.col("status") == "completed")
        .groupBy("user_id")
        .agg(
            F.count("*").alias("total_events"),
            F.sum("amount").alias("total_spent"),
            F.avg("amount").alias("avg_order"),
            F.min("amount").alias("min_order"),
            F.max("amount").alias("max_order"),
        )
    )

    print_sql("BASIC AGGREGATIONS (COUNT, SUM, AVG, MIN, MAX)", result)
    return result


# ==============================================================================
# EXAMPLE 2: COUNT DISTINCT
# ==============================================================================
def example_count_distinct(session):
    """COUNT(DISTINCT ...) for unique counts."""
    events = session.read.table("events")

    result = (events
        .filter(F.col("event_type") == "purchase")
        .groupBy("user_id")
        .agg(
            F.countDistinct("product_id").alias("unique_products"),
            F.countDistinct("session_id").alias("unique_sessions"),
            F.count("*").alias("total_purchases"),
        )
    )

    print_sql("COUNT DISTINCT", result)
    return result


# ==============================================================================
# EXAMPLE 3: Conditional Aggregations (CASE WHEN)
# ==============================================================================
def example_conditional_aggregations(session):
    """Conditional aggregations using CASE WHEN."""
    events = session.read.table("events")

    result = (events
        .groupBy("user_id")
        .agg(
            # Count specific event types
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_count"),
            # Conditional sums
            F.sum(F.when(F.col("amount") > 500, F.col("amount")).otherwise(0)).alias("high_value_total"),
            # Conversion rate
            (F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)) /
             F.count("*") * 100).alias("conversion_rate"),
        )
    )

    print_sql("CONDITIONAL AGGREGATIONS (CASE WHEN)", result)
    return result


# ==============================================================================
# EXAMPLE 4: Window Functions - Running Totals
# ==============================================================================
def example_window_running_total(session):
    """Window functions for running totals."""
    events = session.read.table("events")

    window_spec = Window.partitionBy("user_id").orderBy("event_time")

    result = (events
        .filter(F.col("event_type") == "purchase")
        .withColumn("running_total", F.sum("amount").over(window_spec))
        .withColumn("running_count", F.count("*").over(window_spec))
        .withColumn("running_avg", F.avg("amount").over(window_spec))
        .select("user_id", "event_time", "amount", "running_total", "running_count", "running_avg")
    )

    print_sql("WINDOW FUNCTIONS - Running Totals", result)
    return result


# ==============================================================================
# EXAMPLE 5: Window Functions - ROW_NUMBER/RANK
# ==============================================================================
def example_window_ranking(session):
    """Window functions for ranking."""
    events = session.read.table("events")

    window_spec = Window.partitionBy("user_id").orderBy(F.col("amount").desc())

    result = (events
        .filter(F.col("event_type") == "purchase")
        .withColumn("rank", F.row_number().over(window_spec))
        .filter(F.col("rank") <= 3)  # Top 3 purchases per user
        .select("user_id", "product_id", "amount", "rank")
    )

    print_sql("WINDOW FUNCTIONS - ROW_NUMBER (Top N)", result)
    return result


# ==============================================================================
# EXAMPLE 6: Window Functions - LAG/LEAD
# ==============================================================================
def example_window_lag_lead(session):
    """Window functions for sequential analysis."""
    events = session.read.table("events")

    window_spec = Window.partitionBy("user_id").orderBy("event_time")

    result = (events
        .filter(F.col("event_type") == "purchase")
        .withColumn("prev_amount", F.lag("amount", 1).over(window_spec))
        .withColumn("next_amount", F.lead("amount", 1).over(window_spec))
        .withColumn("amount_change", F.col("amount") - F.lag("amount", 1).over(window_spec))
        .select("user_id", "event_time", "amount", "prev_amount", "next_amount", "amount_change")
    )

    print_sql("WINDOW FUNCTIONS - LAG/LEAD", result)
    return result


# ==============================================================================
# EXAMPLE 7: JOINs
# ==============================================================================
def example_joins(session):
    """Various JOIN operations."""
    events = session.read.table("events")
    users = session.read.table("users")
    products = session.read.table("products")

    # Join events with users and products
    result = (events
        .filter(F.col("event_type") == "purchase")
        .join(users, on="user_id", how="left")
        .join(products, on="product_id", how="left")
        .groupBy("tier", "category")
        .agg(
            F.count("*").alias("purchases"),
            F.sum("amount").alias("revenue"),
            F.avg("amount").alias("avg_order"),
        )
    )

    print_sql("JOIN OPERATIONS (Events + Users + Products)", result)
    return result


# ==============================================================================
# EXAMPLE 8: Subqueries via CTEs
# ==============================================================================
def example_subqueries(session):
    """Subqueries and derived tables via CTE-like patterns."""
    events = session.read.table("events")

    # First, compute user-level stats
    user_stats = (events
        .filter(F.col("event_type") == "purchase")
        .groupBy("user_id")
        .agg(
            F.count("*").alias("purchase_count"),
            F.sum("amount").alias("total_spent"),
        )
    )

    # Then, categorize users
    result = (user_stats
        .withColumn("user_segment",
            F.when(F.col("total_spent") > 1000, "high_value")
             .when(F.col("total_spent") > 500, "medium_value")
             .otherwise("low_value")
        )
        .groupBy("user_segment")
        .agg(
            F.count("*").alias("user_count"),
            F.sum("total_spent").alias("segment_revenue"),
            F.avg("purchase_count").alias("avg_purchases"),
        )
    )

    print_sql("SUBQUERIES (User Segmentation)", result)
    return result


# ==============================================================================
# EXAMPLE 9: Time-based Features
# ==============================================================================
def example_time_features(session):
    """Time-based feature extraction."""
    events = session.read.table("events")

    result = (events
        .filter(F.col("event_type") == "purchase")
        .withColumn("hour_of_day", F.hour("event_time"))
        .withColumn("day_of_week", F.dayofweek("event_time"))
        .withColumn("month", F.month("event_time"))
        .groupBy("user_id")
        .agg(
            # Preferred shopping time
            F.avg("hour_of_day").alias("avg_shopping_hour"),
            # Weekend vs weekday
            F.sum(F.when(F.col("day_of_week").isin([1, 7]), 1).otherwise(0)).alias("weekend_purchases"),
            F.sum(F.when(~F.col("day_of_week").isin([1, 7]), 1).otherwise(0)).alias("weekday_purchases"),
            # Monthly patterns
            F.countDistinct("month").alias("active_months"),
        )
    )

    print_sql("TIME-BASED FEATURES", result)
    return result


# ==============================================================================
# EXAMPLE 10: Complex Real-World Feature View
# ==============================================================================
def example_complex_feature_view(session):
    """Complex real-world feature view combining multiple patterns."""
    events = session.read.table("events")
    users = session.read.table("users")

    # Define window for recency calculations
    user_window = Window.partitionBy("user_id").orderBy(F.col("event_time").desc())

    # Build comprehensive user features
    result = (events
        # Filter to relevant events
        .filter(F.col("status") == "completed")

        # Join with user profile
        .join(users, on="user_id", how="left")

        # Add recency info
        .withColumn("event_rank", F.row_number().over(user_window))

        # Aggregate per user
        .groupBy("user_id", "tier", "country")
        .agg(
            # Basic counts
            F.count("*").alias("total_events"),
            F.countDistinct("product_id").alias("unique_products"),
            F.countDistinct("session_id").alias("unique_sessions"),

            # Purchase metrics
            F.sum(F.when(F.col("event_type") == "purchase", F.col("amount")).otherwise(0)).alias("total_revenue"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
            F.avg(F.when(F.col("event_type") == "purchase", F.col("amount"))).alias("avg_order_value"),

            # Engagement metrics
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_adds"),

            # Conversion funnel
            (F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)) * 100.0 /
             F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0))).alias("view_to_purchase_rate"),

            # Time patterns
            F.min("event_time").alias("first_event"),
            F.max("event_time").alias("last_event"),

            # High-value purchases
            F.sum(F.when((F.col("event_type") == "purchase") & (F.col("amount") > 500), 1).otherwise(0)).alias("premium_purchases"),
        )
    )

    print_sql("COMPLEX REAL-WORLD FEATURE VIEW", result)
    return result


# ==============================================================================
# EXAMPLE 11: Pivoting / Cross-tabulation
# ==============================================================================
def example_pivot(session):
    """Pivot table for cross-tabulation features."""
    events = session.read.table("events")
    products = session.read.table("products")

    # Create category purchase counts per user (manual pivot)
    result = (events
        .filter(F.col("event_type") == "purchase")
        .join(products, on="product_id", how="left")
        .groupBy("user_id")
        .agg(
            F.sum(F.when(F.col("category") == "electronics", 1).otherwise(0)).alias("electronics_purchases"),
            F.sum(F.when(F.col("category") == "clothing", 1).otherwise(0)).alias("clothing_purchases"),
            F.sum(F.when(F.col("category") == "home", 1).otherwise(0)).alias("home_purchases"),
            F.sum(F.when(F.col("category") == "electronics", F.col("amount")).otherwise(0)).alias("electronics_spend"),
            F.sum(F.when(F.col("category") == "clothing", F.col("amount")).otherwise(0)).alias("clothing_spend"),
            F.sum(F.when(F.col("category") == "home", F.col("amount")).otherwise(0)).alias("home_spend"),
        )
    )

    print_sql("PIVOT / CROSS-TABULATION", result)
    return result


# ==============================================================================
# EXAMPLE 12: HAVING clause (Filter after aggregation)
# ==============================================================================
def example_having(session):
    """Filter aggregated results (equivalent to HAVING clause)."""
    events = session.read.table("events")

    result = (events
        .filter(F.col("event_type") == "purchase")
        .groupBy("user_id")
        .agg(
            F.count("*").alias("purchase_count"),
            F.sum("amount").alias("total_spent"),
        )
        # Filter AFTER aggregation (HAVING clause)
        .filter(F.col("purchase_count") >= 5)
        .filter(F.col("total_spent") > 1000)
    )

    print_sql("HAVING CLAUSE (Filter after aggregation)", result)
    return result


# ==============================================================================
# MAIN: Run all examples
# ==============================================================================
def main():
    print("\n" + "="*80)
    print(" FEATUREDUCK: PySpark → DuckDB SQL Generation Examples")
    print(" Using SQLFrame for optimal SQL conversion")
    print("="*80)

    session = setup_session()

    # Run all examples
    example_basic_aggregations(session)
    example_count_distinct(session)
    example_conditional_aggregations(session)
    example_window_running_total(session)
    example_window_ranking(session)
    example_window_lag_lead(session)
    example_joins(session)
    example_subqueries(session)
    example_time_features(session)
    example_complex_feature_view(session)
    example_pivot(session)
    example_having(session)

    print("\n" + "="*80)
    print(" All examples completed!")
    print(" Total: 12 SQL patterns demonstrated")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
