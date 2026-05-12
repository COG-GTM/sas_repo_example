"""Shared pytest fixtures for PySpark migration tests."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("MEPS_PySpark_Migration_Tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    yield session
    session.stop()
