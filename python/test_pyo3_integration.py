"""Test PyO3 integration - Python → Rust → SQL compilation"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

import json
from featureduck import col, count, sum_, days_ago, DataFrame, DeltaSource
from featureduck.featureduck_native import compile_to_duckdb_sql


def test_simple_filter_compilation():
    """Test Python → Rust → DuckDB SQL compilation"""
    source = {"type": "delta", "path": "s3://bucket/events"}
    
    df = DataFrame(
        _source=source,
        _filters=[col("event_type") == "click"],
        _group_by_cols=[],
        _aggregations=[],
        _order_by=[],
        _limit_val=None
    )
    
    plan_dict = df.to_logical_plan()
    plan_json = json.dumps(plan_dict)
    
    print("LogicalPlan JSON:")
    print(plan_json)
    print()
    
    sql = compile_to_duckdb_sql(plan_json)
    
    print("Generated DuckDB SQL:")
    print(sql)
    print()
    
    assert "SELECT *" in sql
    assert "FROM read_parquet" in sql
    assert "event_type = 'click'" in sql
    
    print("✅ Simple filter test passed!")


def test_aggregation_compilation():
    """Test aggregation query compilation"""
    source = {"type": "delta", "path": "s3://bucket/events"}
    
    df = DataFrame(
        _source=source,
        _filters=[col("timestamp") >= days_ago(7)],
        _group_by_cols=["user_id"],
        _aggregations=[
            count().alias("clicks_7d"),
            sum_("amount").alias("total_amount")
        ],
        _order_by=[],
        _limit_val=None
    )
    
    plan_dict = df.to_logical_plan()
    plan_json = json.dumps(plan_dict)
    
    print("LogicalPlan JSON:")
    print(json.dumps(plan_dict, indent=2))
    print()
    
    sql = compile_to_duckdb_sql(plan_json)
    
    print("Generated DuckDB SQL:")
    print(sql)
    print()
    
    assert "COUNT(*) AS clicks_7d" in sql
    assert "SUM(amount) AS total_amount" in sql
    assert "GROUP BY user_id" in sql
    assert "DAYS_AGO(7)" in sql or "INTERVAL" in sql
    
    print("✅ Aggregation test passed!")


def test_complex_transformation():
    """Test complex transformation with multiple operations"""
    source = {"type": "delta", "path": "s3://bucket/events"}
    
    df = (DataFrame(_source=source)
          .filter(col("timestamp") >= days_ago(7))
          .filter(col("event_type") == "purchase")
          .group_by("user_id", "category")
          .agg(
              count().alias("purchases"),
              sum_("amount").alias("total_spent")
          )
          .order_by("purchases", ascending=False)
          .limit(100))
    
    plan_dict = df.to_logical_plan()
    plan_json = json.dumps(plan_dict)
    
    sql = compile_to_duckdb_sql(plan_json)
    
    print("Generated DuckDB SQL:")
    print(sql)
    print()
    
    assert "GROUP BY user_id, category" in sql
    assert "ORDER BY purchases DESC" in sql
    assert "LIMIT 100" in sql
    
    print("✅ Complex transformation test passed!")


if __name__ == "__main__":
    print("=" * 60)
    print("Testing PyO3 Integration: Python → Rust → SQL")
    print("=" * 60)
    print()
    
    try:
        test_simple_filter_compilation()
        print()
        test_aggregation_compilation()
        print()
        test_complex_transformation()
        print()
        print("=" * 60)
        print("🎉 All PyO3 integration tests passed!")
        print("=" * 60)
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
