"""Spark session management and common utilities for MEPS PySpark Migration."""

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "MEPS_PySpark_Migration") -> SparkSession:
    """Create or get existing Spark session configured for MEPS analysis.

    Args:
        app_name: Application name for the Spark session.

    Returns:
        Configured SparkSession instance.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def read_meps_file(spark: SparkSession, file_path: str) -> "DataFrame":
    """Read a MEPS data file (Parquet, CSV, or SAS transport).

    Supports Parquet (preferred), CSV, and attempts to handle common
    MEPS file formats.

    Args:
        spark: Active SparkSession.
        file_path: Path to the data file.

    Returns:
        Spark DataFrame with the loaded data.
    """
    if file_path.endswith(".parquet") or file_path.endswith("/"):
        return spark.read.parquet(file_path)
    elif file_path.endswith(".csv"):
        return spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
    else:
        return spark.read.parquet(file_path)
