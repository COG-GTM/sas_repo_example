"""
Data loading utilities for MEPS data in Parquet format.

This module provides functions to load MEPS data from Parquet files
with optimized column pruning and filtering capabilities.
"""

from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession


def get_spark_session(app_name="MEPS_Analysis"):
    """
    Get or create a Spark session with optimized configurations.

    Args:
        app_name (str): Name for the Spark application

    Returns:
        SparkSession: Configured Spark session
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.autoBroadcastJoinThreshold", "10485760")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.files.maxPartitionBytes", "128MB")
        .getOrCreate()
    )
    return spark


def load_meps_parquet(
    parquet_path: str,
    columns: Optional[List[str]] = None,
    spark: Optional[SparkSession] = None,
) -> DataFrame:
    """
    Load MEPS data from Parquet file with optional column pruning.

    Args:
        parquet_path (str): Path to the Parquet file
        columns (List[str], optional): List of columns to load.
            If None, loads all columns.
        spark (SparkSession, optional): Existing Spark session.
            If None, creates new one.

    Returns:
        DataFrame: Spark DataFrame containing the MEPS data
    """
    if spark is None:
        spark = get_spark_session()

    if columns:
        df = spark.read.parquet(parquet_path).select(*columns)
    else:
        df = spark.read.parquet(parquet_path)

    return df


def load_h192_for_exercise_1a(
    parquet_path: str, spark: Optional[SparkSession] = None
) -> DataFrame:
    """
    Load H192 (2016 Full-Year Consolidated) data with columns for Exercise 1a.

    This function loads only the required columns for Exercise 1a analysis:
    - DUPERSID: Person ID
    - TOTEXP16: Total healthcare expenditure in 2016
    - AGE16X: Age as of 12/31/2016
    - AGE42X: Age as of 4/30/2016
    - AGE31X: Age as of 3/31/2016
    - VARSTR: Variance estimation stratum
    - VARPSU: Variance estimation PSU
    - PERWT16F: Person weight for 2016

    Args:
        parquet_path (str): Path to the H192 Parquet file
        spark (SparkSession, optional): Existing Spark session.
            If None, creates new one.

    Returns:
        DataFrame: Spark DataFrame with required columns
    """
    required_columns = [
        "DUPERSID",
        "TOTEXP16",
        "AGE16X",
        "AGE42X",
        "AGE31X",
        "VARSTR",
        "VARPSU",
        "PERWT16F",
    ]

    return load_meps_parquet(parquet_path, columns=required_columns, spark=spark)


def validate_meps_data(df: DataFrame, required_columns: List[str]) -> bool:
    """
    Validate that a MEPS DataFrame contains all required columns.

    Args:
        df (DataFrame): DataFrame to validate
        required_columns (List[str]): List of required column names

    Returns:
        bool: True if all required columns are present, False otherwise
    """
    df_columns = set(df.columns)
    missing_columns = set(required_columns) - df_columns

    if missing_columns:
        print(f"Warning: Missing required columns: {missing_columns}")
        return False

    return True
