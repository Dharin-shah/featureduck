"""
FeatureDuck Feature View with PySpark API.

This module provides a PySpark-compatible API for defining feature transformations
using SQLFrame (a library providing PySpark compatibility for DuckDB/Spark).
"""

from typing import Dict, Any, List, Optional, Callable
import warnings

# SQLFrame will be an optional dependency
try:
    from sqlframe.duckdb import DuckDBSession
    from pyspark.sql import DataFrame as PySparkDataFrame
    from pyspark.sql import functions as F  # noqa: F401
    from pyspark.sql import Window  # noqa: F401
    SQLFRAME_AVAILABLE = True
except ImportError:
    SQLFRAME_AVAILABLE = False
    warnings.warn(
        "SQLFrame not available. Install with: pip install 'sqlframe[duckdb]'",
        ImportWarning
    )


class FeatureView:
    """
    PySpark-compatible feature view using SQLFrame.

    Supports DuckDB (local) and Spark (distributed) backends
    without code changes.

    Examples:
        >>> from featureduck import FeatureView, F
        >>>
        >>> # Create view with DuckDB backend
        >>> view = FeatureView(
        ...     name="user_purchases",
        ...     entities=["user_id"],
        ...     engine="duckdb"
        ... )
        >>>
        >>> # Read source data
        >>> df = view.read_source({
        ...     "type": "parquet",
        ...     "path": "s3://bucket/events"
        ... })
        >>>
        >>> # Apply PySpark transformations
        >>> features = (df
        ...     .filter(F.col("event_type") == "purchase")
        ...     .groupBy("user_id")
        ...     .agg(
        ...         F.count("*").alias("purchase_count"),
        ...         F.sum("amount").alias("total_revenue")
        ...     )
        ... )
        >>>
        >>> # Get SQL for inspection
        >>> sql = view.get_sql(features)
        >>> print(sql)
        >>>
        >>> # Materialize to Delta Lake (via Rust core)
        >>> view.materialize(
        ...     features,
        ...     output_path="s3://bucket/features/user_purchases"
        ... )

    Attributes:
        name (str): Feature view name
        entities (List[str]): Entity key columns
        engine (str): Backend engine ("duckdb" or "spark")
        session: SQLFrame session for the chosen engine
    """

    def __init__(
        self,
        name: str,
        entities: List[str],
        engine: str = "duckdb",
        config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize feature view with SQLFrame backend.

        Args:
            name: Feature view name (must be unique)
            entities: Entity key columns (e.g., ["user_id", "product_id"])
            engine: Backend engine - "duckdb" (local) or "spark" (distributed)
            config: Optional engine-specific configuration dict

        Raises:
            ImportError: If SQLFrame is not installed
            ValueError: If engine is not supported

        Example:
            >>> view = FeatureView(
            ...     name="user_features",
            ...     entities=["user_id"],
            ...     engine="duckdb",
            ...     config={"threads": 4}
            ... )
        """
        if not SQLFRAME_AVAILABLE:
            raise ImportError(
                "SQLFrame is required but not installed. "
                "Install with: pip install 'sqlframe[duckdb]'"
            )

        self.name = name
        self.entities = entities
        self.engine = engine
        self.config = config or {}
        self.session = self._create_session()

    def _create_session(self):
        """
        Create SQLFrame session based on engine type.

        Returns:
            SQLFrame session (DuckDBSession or SparkSession)

        Raises:
            ValueError: If engine is not supported
        """
        if self.engine == "duckdb":
            from sqlframe.duckdb import DuckDBSession
            builder = DuckDBSession.builder

            # Apply config options
            for key, value in self.config.items():
                builder = builder.config(key, value)

            return builder.getOrCreate()

        elif self.engine == "spark":
            from sqlframe.spark import SparkSession
            builder = SparkSession.builder

            # Apply config options
            for key, value in self.config.items():
                builder = builder.config(key, value)

            return builder.getOrCreate()

        else:
            raise ValueError(
                f"Unsupported engine: {self.engine}. "
                f"Supported engines: duckdb, spark"
            )

    def read_source(
        self,
        source_config: Dict[str, Any]
    ) -> 'PySparkDataFrame':
        """
        Read source data using PySpark API.

        Args:
            source_config: Source configuration dict with keys:
                - type: Data format ("parquet", "delta", "csv", "json")
                - path: Data path (local or S3/GCS/Azure)
                - options: Optional read options dict

        Returns:
            PySpark DataFrame

        Raises:
            ValueError: If source type is not supported
            FileNotFoundError: If source path doesn't exist

        Example:
            >>> df = view.read_source({
            ...     "type": "parquet",
            ...     "path": "s3://bucket/events",
            ...     "options": {"compression": "snappy"}
            ... })
        """
        source_type = source_config["type"]
        path = source_config["path"]
        options = source_config.get("options", {})

        reader = self.session.read

        # Apply read options
        for key, value in options.items():
            reader = reader.option(key, value)

        # Read based on format type
        if source_type == "parquet":
            return reader.parquet(path)
        elif source_type == "delta":
            return reader.format("delta").load(path)
        elif source_type == "csv":
            return reader.csv(path, header=True, inferSchema=True)
        elif source_type == "json":
            return reader.json(path)
        else:
            raise ValueError(
                f"Unsupported source type: {source_type}. "
                f"Supported types: parquet, delta, csv, json"
            )

    def transform(
        self,
        source_df: 'PySparkDataFrame',
        transformation_func: Callable[['PySparkDataFrame'], 'PySparkDataFrame'],
    ) -> 'PySparkDataFrame':
        """
        Apply user-defined transformation using PySpark API.

        Args:
            source_df: Input DataFrame from read_source()
            transformation_func: Function that transforms DataFrame

        Returns:
            Transformed DataFrame

        Example:
            >>> def my_transform(df):
            ...     return (df
            ...         .filter(F.col("amount") > 100)
            ...         .groupBy("user_id")
            ...         .agg(F.sum("amount").alias("total"))
            ...     )
            >>>
            >>> df = view.read_source({"type": "parquet", "path": "..."})
            >>> features = view.transform(df, my_transform)
        """
        return transformation_func(source_df)

    def get_sql(
        self,
        df: 'PySparkDataFrame',
        optimize: bool = True,
    ) -> str:
        """
        Extract SQL from DataFrame for inspection or execution.

        This is useful for:
        - Debugging transformations
        - Verifying generated SQL quality
        - Executing SQL in Rust core

        Args:
            df: PySpark DataFrame (from transformation)
            optimize: Whether to generate optimized/readable SQL

        Returns:
            SQL string in the target dialect (DuckDB or Spark)

        Example:
            >>> features = df.groupBy("user_id").agg(F.count("*"))
            >>> sql = view.get_sql(features, optimize=True)
            >>> print(sql)
            SELECT user_id, COUNT(*) AS count
            FROM ...
            GROUP BY user_id
        """
        # Use SQLFrame's dialect system for proper SQL generation
        # This generates correct SQL for the target engine without string hacks
        if hasattr(df, 'expression'):
            # SQLFrame DataFrame - use proper dialect transpilation
            expr = df.expression
            dialect = 'duckdb' if self.engine == 'duckdb' else 'spark'
            return expr.sql(dialect=dialect)
        else:
            # Standard PySpark DataFrame - use default SQL generation
            return df.sql(optimize=optimize)

    def materialize(
        self,
        df: 'PySparkDataFrame',
        output_path: str,
        mode: str = "overwrite",
        use_rust_core: bool = True,
        registry_client = None,
    ) -> Dict[str, Any]:
        """
        Materialize features to Delta Lake.

        This method:
        1. Extracts SQL from DataFrame
        2. Calls Rust core to execute SQL (if use_rust_core=True)
        3. Writes results to Delta Lake
        4. Updates registry with stats (if provided)

        Args:
            df: Transformed DataFrame to materialize
            output_path: Delta Lake output path
            mode: Write mode ("overwrite", "append")
            use_rust_core: If True, use Rust execute_and_write for performance
            registry_client: Optional registry for metadata updates

        Returns:
            Dict with materialization results:
                - status: "success" or "error"
                - rows_written: Number of rows materialized
                - output_path: Where data was written
                - sql: SQL query executed

        Example:
            >>> result = view.materialize(
            ...     features,
            ...     output_path="s3://bucket/features/user_features",
            ...     mode="overwrite"
            ... )
            >>> print(f"Wrote {result['rows_written']} rows")
        """
        # Extract SQL from DataFrame using proper dialect system
        # get_sql() now generates correct DuckDB SQL without string hacks
        sql = self.get_sql(df, optimize=True)

        # Use Rust core for execution and writing (faster, production-ready)
        if use_rust_core:
            try:
                from featureduck.featureduck_native import execute_and_write

                # Execute SQL and write to Delta Lake via Rust
                result = execute_and_write(
                    sql=sql,
                    output_path=output_path,
                    feature_view_name=self.name,
                    entity_columns=self.entities,
                    mode=mode
                )

                # Add SQL to result for debugging
                result["sql"] = sql
                result["mode"] = mode

                return result

            except ImportError:
                # Fallback to Python execution if Rust native module not available
                import warnings
                warnings.warn(
                    "Rust native module not available. Falling back to Python execution. "
                    "For better performance, ensure featureduck native module is installed.",
                    RuntimeWarning
                )
                use_rust_core = False

        # Fallback: Execute in Python (slower, for testing/development)
        if not use_rust_core:
            try:
                df.write.format("delta").mode(mode).save(output_path)

                # Count rows (may be slow for large datasets)
                row_count = df.count()

                return {
                    "status": "success",
                    "rows_written": row_count,
                    "output_path": output_path,
                    "mode": mode,
                    "sql": sql,
                }
            except Exception as e:
                return {
                    "status": "error",
                    "error": str(e),
                    "output_path": output_path,
                    "sql": sql,
                }

    def __repr__(self) -> str:
        """String representation of feature view."""
        return (
            f"FeatureView("
            f"name='{self.name}', "
            f"entities={self.entities}, "
            f"engine='{self.engine}')"
        )
