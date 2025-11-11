"""
Configuration module for MEPS PySpark migration.

This module provides configuration classes and constants for file paths,
Spark settings, and survey design variables.
"""

from pyspark_migration.config.paths import (
    DEFAULT_PATHS,
    MEPSPaths,
    SPARK_CONFIGS,
    SURVEY_DESIGN_VARS,
)

__all__ = [
    "MEPSPaths",
    "DEFAULT_PATHS",
    "SPARK_CONFIGS",
    "SURVEY_DESIGN_VARS",
]
