"""
FeatureDuck Python SDK

High-performance, engine-agnostic feature transformation API.

Provides Tecton-style declarative API and PySpark transformations.

Example (Tecton-style):

    from featureduck import FeatureDefinition, Entity, DataSource
    from pyspark.sql import functions as F

    user = Entity(name="user", join_keys=["user_id"])
    events = DataSource(name="events", source_type="delta", path="s3://bucket/events")

    @FeatureDefinition(name="user_features", entities=[user], source=events)
    def user_features(df):
        return df.groupBy("user_id").agg(F.count("*").alias("event_count"))

    # Apply to registry and materialize via CLI
    user_features.apply()
    # Then: featureduck materialize --name user_features
"""

__version__ = "0.1.0"

# ============================================================================
# MAIN API: Tecton-style feature definitions
# ============================================================================

from .feature_definition import (
    FeatureDefinition,
    Entity,
    DataSource,
    feature_view,
)

# Interactive feature view (SQLFrame backend)
from .sqlframe_view import FeatureView

# Re-export PySpark components for convenience
try:
    from pyspark.sql import functions as F
    from pyspark.sql import Window
    PYSPARK_AVAILABLE = True
except ImportError:
    # PySpark not available - SQLFrame will handle this
    F = None
    Window = None
    PYSPARK_AVAILABLE = False


# ============================================================================
# VALIDATION API: Tecton-style feature schema validation
# ============================================================================

from . import validation

# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    # Tecton-style definitions
    "FeatureDefinition",  # Decorator for defining feature views
    "Entity",             # Entity definition
    "DataSource",         # Data source definition
    "feature_view",       # Programmatic feature view creation
    # Interactive mode
    "FeatureView",        # Interactive PySpark-style transformations
    # PySpark compatibility
    "F",                  # PySpark functions
    "Window",             # PySpark window functions
    # Validation
    "validation",         # Schema validation module
]
