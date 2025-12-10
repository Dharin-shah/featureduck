"""
Tecton-style Feature Definitions for FeatureDuck.

This module provides a declarative API for defining feature views using PySpark
transformations. Features are defined in Python, applied to the registry, and
then materialized via CLI.

Example usage:

    from featureduck import FeatureDefinition, Entity, DataSource
    from sqlframe.duckdb import functions as F

    # Define entities
    user = Entity(name="user", join_keys=["user_id"])

    # Define data source
    events = DataSource(
        name="events",
        source_type="delta",
        path="s3://bucket/events",
        timestamp_field="event_time"
    )

    # Define feature view with PySpark transformations
    @FeatureDefinition(
        name="user_purchase_features",
        entities=[user],
        source=events,
        ttl_days=7,
    )
    def user_purchases(df):
        return (df
            .filter(F.col("event_type") == "purchase")
            .groupBy("user_id")
            .agg(
                F.count("*").alias("purchase_count"),
                F.sum("amount").alias("total_spent"),
                F.avg("amount").alias("avg_order_value"),
                F.countDistinct("product_id").alias("unique_products")
            )
        )

    # Apply to registry
    user_purchases.apply()

    # Or get SQL for dry-run
    print(user_purchases.to_sql())
"""

from typing import List, Optional, Callable, Dict, Any, Union
from dataclasses import dataclass, field
from pathlib import Path
import json
import logging
import re

# Pre-compiled regex patterns for performance
_TABLE_ALIAS_PATTERN = re.compile(r'\bt\d+\b')
_FROM_SRC_PATTERN = re.compile(r'\bFROM\s+src\b')

logger = logging.getLogger(__name__)


class FeatureDefinitionError(Exception):
    """Exception raised for feature definition errors."""
    pass


@dataclass
class Entity:
    """Entity definition for feature views.

    Attributes:
        name: Human-readable name for the entity
        join_keys: List of column names used as join keys
        description: Optional description of the entity
    """
    name: str
    join_keys: List[str]
    description: str = ""

    def __post_init__(self):
        """Validate entity configuration."""
        if not self.name:
            raise ValueError("Entity name cannot be empty")
        if not self.join_keys:
            raise ValueError(f"Entity '{self.name}' must have at least one join key")
        for key in self.join_keys:
            if not key:
                raise ValueError(f"Entity '{self.name}' has empty join key")


@dataclass
class DataSource:
    """Data source definition.

    Attributes:
        name: Human-readable name for the data source
        source_type: Type of source ("delta", "parquet", "csv")
        path: Path to the data source (local or cloud)
        timestamp_field: Name of the timestamp column
        description: Optional description
        options: Additional options for reading the source
    """
    name: str
    source_type: str  # "delta", "parquet", "csv"
    path: str
    timestamp_field: str = "timestamp"
    description: str = ""
    options: Dict[str, Any] = field(default_factory=dict)

    # Valid source types
    VALID_SOURCE_TYPES = frozenset({"delta", "parquet", "csv", "json"})

    def __post_init__(self):
        """Validate data source configuration."""
        if not self.name:
            raise ValueError("DataSource name cannot be empty")
        if not self.path:
            raise ValueError(f"DataSource '{self.name}' path cannot be empty")
        if self.source_type not in self.VALID_SOURCE_TYPES:
            raise ValueError(
                f"DataSource '{self.name}' has invalid source_type '{self.source_type}'. "
                f"Must be one of: {sorted(self.VALID_SOURCE_TYPES)}"
            )


class FeatureDefinition:
    """
    Tecton-style decorator for defining feature views.

    This class can be used as a decorator to define feature transformations
    using PySpark-style DataFrame API. The transformations are converted to
    SQL using SQLFrame for execution on DuckDB.

    Example:
        @FeatureDefinition(
            name="user_features",
            entities=[user_entity],
            source=events_source,
            ttl_days=7,
        )
        def user_features(df):
            return df.groupBy("user_id").agg(F.count("*").alias("event_count"))

    Attributes:
        name: Unique name for the feature view
        entities: List of Entity objects defining join keys
        source: DataSource object defining input data
        ttl_days: Optional TTL for feature freshness
        batch_schedule: Optional cron schedule for batch processing
        description: Human-readable description
        tags: Optional list of tags for organization
        owner: Optional owner identifier
    """

    def __init__(
        self,
        name: str,
        entities: List[Entity],
        source: DataSource,
        ttl_days: Optional[int] = None,
        batch_schedule: Optional[str] = None,
        description: str = "",
        tags: Optional[List[str]] = None,
        owner: Optional[str] = None,
    ):
        # Validate inputs
        if not name:
            raise ValueError("Feature view name cannot be empty")
        if not name.replace("_", "").replace("-", "").isalnum():
            raise ValueError(
                f"Feature view name '{name}' must contain only alphanumeric "
                "characters, underscores, and hyphens"
            )
        if not entities:
            raise ValueError(f"Feature view '{name}' must have at least one entity")
        if source is None:
            raise ValueError(f"Feature view '{name}' must have a data source")
        if ttl_days is not None and ttl_days < 0:
            raise ValueError(f"Feature view '{name}' ttl_days must be non-negative")

        self.name = name
        self.entities = entities
        self.source = source
        self.ttl_days = ttl_days
        self.batch_schedule = batch_schedule
        self.description = description
        self.tags = tags or []
        self.owner = owner
        self._transform_func: Optional[Callable] = None
        self._sql: Optional[str] = None
        self._sql_extraction_error: Optional[str] = None

    def __call__(self, func: Callable) -> 'FeatureDefinition':
        """Decorator that captures the transformation function."""
        if not callable(func):
            raise TypeError(f"Expected callable, got {type(func).__name__}")
        self._transform_func = func
        self._extract_sql()
        return self

    def _extract_sql(self) -> None:
        """
        Extract SQL from the transformation using SQLFrame's dialect system.

        Uses SQLFrame to convert PySpark-style transformations to DuckDB SQL
        via sqlglot's dialect transpilation. Falls back to template if SQLFrame
        is not available or extraction fails.
        """
        conn = None
        try:
            from sqlframe.duckdb import DuckDBSession
            import duckdb

            # Create DuckDB connection
            conn = duckdb.connect()

            # Create source reference for final SQL
            source_ref = self._get_source_ref()

            # Create a mock table with schema that matches source data
            entity_col = self.entities[0].join_keys[0]
            ts_field = self.source.timestamp_field

            # Use 'src' as the table name from the start for consistency
            conn.execute(f"""
                CREATE OR REPLACE TABLE src AS
                SELECT
                    'entity_value'::VARCHAR AS {entity_col},
                    TIMESTAMP '2024-01-01' AS {ts_field},
                    'completed'::VARCHAR AS status,
                    'purchase'::VARCHAR AS event_type,
                    100.0::DOUBLE AS amount,
                    'prod_1'::VARCHAR AS product_id
                WHERE FALSE
            """)

            # Create SQLFrame session with the DuckDB connection
            session = DuckDBSession.builder.config("conn", conn).getOrCreate()

            # Read mock table through SQLFrame
            mock_df = session.read.table("src")

            # Apply the user's transformation function
            if self._transform_func:
                result_df = self._transform_func(mock_df)

                # Use SQLFrame's dialect system for clean SQL generation
                expr = result_df.expression

                # Generate SQL using DuckDB dialect
                duckdb_sql = expr.sql(dialect='duckdb')

                # Replace 'src' table reference with actual source
                clean_sql = _FROM_SRC_PATTERN.sub(
                    f'FROM {source_ref} AS src',
                    duckdb_sql,
                    count=1  # Only replace the first FROM
                )

                # Handle CTEs: replace any table aliases like t12345678 with src
                clean_sql = _TABLE_ALIAS_PATTERN.sub('src', clean_sql)

                self._sql = clean_sql
                self._sql_extraction_error = None

        except ImportError as e:
            # SQLFrame not available - generate SQL template instead
            logger.warning(f"SQLFrame not available, using template: {e}")
            self._sql = self._generate_sql_template()
            self._sql_extraction_error = f"SQLFrame not available: {e}"
        except Exception as e:
            # Fallback to template if SQLFrame fails
            logger.warning(f"SQL extraction failed for '{self.name}': {e}")
            self._sql = self._generate_sql_template()
            self._sql_extraction_error = str(e)
        finally:
            # Always clean up connection
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def _get_source_ref(self) -> str:
        """Get DuckDB source reference for this data source."""
        source_type = self.source.source_type
        path = self.source.path

        if source_type == "delta":
            return f"delta_scan('{path}')"
        elif source_type == "parquet":
            return f"read_parquet('{path}/**/*.parquet')"
        elif source_type == "csv":
            return f"read_csv('{path}')"
        elif source_type == "json":
            return f"read_json('{path}')"
        else:
            # Fallback - treat as table name
            return f"'{path}'"

    def _generate_sql_template(self) -> str:
        """Generate SQL template with placeholders when SQLFrame is unavailable."""
        entity_cols = ", ".join(e.join_keys[0] for e in self.entities)
        ts_field = self.source.timestamp_field
        source_ref = self._get_source_ref()

        return f"""SELECT {entity_cols}, *
FROM {source_ref}
WHERE {ts_field} >= '{{{{start_time}}}}'
  AND {ts_field} < '{{{{end_time}}}}'
GROUP BY {entity_cols}"""

    def to_sql(
        self,
        start_time: str = "{{start_time}}",
        end_time: str = "{{end_time}}"
    ) -> str:
        """
        Get the SQL query for this feature definition.

        Args:
            start_time: Start timestamp (or placeholder for CLI substitution)
            end_time: End timestamp (or placeholder for CLI substitution)

        Returns:
            SQL query string ready for execution
        """
        if self._sql:
            sql = self._sql
            # Replace time placeholders if present in the SQL
            sql = sql.replace("{{start_time}}", start_time)
            sql = sql.replace("{{end_time}}", end_time)
            return sql
        return self._generate_sql_template()

    def to_registry_dict(self) -> Dict[str, Any]:
        """
        Convert to registry-compatible dict for CLI.

        Returns:
            Dict that can be serialized to JSON for the registry
        """
        return {
            "name": self.name,
            "version": 1,
            "source_type": self.source.source_type,
            "source_path": self.source.path,
            "entities": [e.join_keys[0] for e in self.entities],
            "transformations": self._sql or self._generate_sql_template(),
            "timestamp_field": self.source.timestamp_field,
            "ttl_days": self.ttl_days,
            "batch_schedule": self.batch_schedule,
            "description": self.description,
            "tags": self.tags,
            "owner": self.owner,
        }

    def apply(self, registry_path: str = ".featureduck/registry.db") -> bool:
        """
        Apply this feature definition to the registry.

        This registers the feature view so it can be materialized via CLI.

        Args:
            registry_path: Path to SQLite registry database

        Returns:
            True if successful

        Raises:
            FeatureDefinitionError: If registration fails
        """
        import sqlite3
        from datetime import datetime

        conn = None
        try:
            # Ensure parent directory exists
            registry_file = Path(registry_path)
            registry_file.parent.mkdir(parents=True, exist_ok=True)

            # Connect to registry
            conn = sqlite3.connect(str(registry_file))
            cursor = conn.cursor()

            # Ensure schema exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS feature_views (
                    name TEXT PRIMARY KEY,
                    version INTEGER NOT NULL,
                    source_type TEXT NOT NULL,
                    source_path TEXT NOT NULL,
                    entities TEXT NOT NULL,
                    transformations TEXT NOT NULL,
                    timestamp_field TEXT,
                    ttl_days INTEGER,
                    batch_schedule TEXT,
                    description TEXT,
                    tags TEXT,
                    owner TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)

            # Insert or update
            data = self.to_registry_dict()
            now = datetime.utcnow().isoformat()

            cursor.execute("""
                INSERT OR REPLACE INTO feature_views
                (name, version, source_type, source_path, entities, transformations,
                 timestamp_field, ttl_days, batch_schedule, description, tags, owner,
                 created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data["name"],
                data["version"],
                data["source_type"],
                data["source_path"],
                json.dumps(data["entities"]),
                data["transformations"],
                data.get("timestamp_field"),
                data.get("ttl_days"),
                data.get("batch_schedule"),
                data.get("description"),
                json.dumps(data.get("tags", [])),
                data.get("owner"),
                now,
                now,
            ))

            conn.commit()
            logger.info(f"Applied feature view '{self.name}' to registry at {registry_path}")
            print(f"Applied feature view '{self.name}' to registry")
            return True

        except sqlite3.Error as e:
            raise FeatureDefinitionError(
                f"Failed to apply feature view '{self.name}' to registry: {e}"
            ) from e
        except Exception as e:
            raise FeatureDefinitionError(
                f"Unexpected error applying feature view '{self.name}': {e}"
            ) from e
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def __repr__(self) -> str:
        return f"FeatureDefinition(name='{self.name}', entities={[e.name for e in self.entities]})"


def feature_view(
    name: str,
    entities: List[Entity],
    source: DataSource,
    transform: Callable,
    **kwargs
) -> FeatureDefinition:
    """
    Create a feature view programmatically (non-decorator style).

    This is useful when you want to create feature views dynamically
    or when the decorator syntax doesn't fit your use case.

    Example:
        view = feature_view(
            name="user_features",
            entities=[user],
            source=events,
            transform=lambda df: df.groupBy("user_id").agg(F.count("*").alias("count")),
            ttl_days=7,
        )
        view.apply()

    Args:
        name: Unique name for the feature view
        entities: List of Entity objects
        source: DataSource object
        transform: Transformation function (df -> df)
        **kwargs: Additional arguments passed to FeatureDefinition

    Returns:
        Configured FeatureDefinition instance
    """
    fd = FeatureDefinition(name=name, entities=entities, source=source, **kwargs)
    return fd(transform)
