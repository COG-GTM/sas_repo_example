"""Utility functions shared across PySpark ETL jobs."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit
import os


def get_or_create_spark(app_name: str = "MEPS_PySpark") -> SparkSession:
    """Get or create a SparkSession."""
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def read_input(spark: SparkSession, file_code: str, data_path: str = None) -> DataFrame:
    """Read a MEPS input file. Supports Parquet, CSV, and SAS7BDAT (via pandas)."""
    if data_path is None:
        data_path = os.environ.get("MEPS_DATA_PATH", "data/meps")

    parquet_path = os.path.join(data_path, file_code, "")
    csv_path = os.path.join(data_path, f"{file_code}.csv")

    if os.path.exists(parquet_path):
        return spark.read.parquet(parquet_path)
    elif os.path.exists(csv_path):
        return spark.read.csv(csv_path, header=True, inferSchema=True)
    else:
        raise FileNotFoundError(
            f"No data file found for {file_code} at {data_path}. "
            f"Expected Parquet dir at {parquet_path} or CSV at {csv_path}."
        )


def write_output(df: DataFrame, job_name: str, output_path: str = None) -> str:
    """Write a DataFrame to Parquet. Returns the output path."""
    if output_path is None:
        output_path = os.environ.get("MEPS_OUTPUT_PATH", "data/processed")

    full_path = os.path.join(output_path, job_name)
    df.write.mode("overwrite").parquet(full_path)
    return full_path


def validate_survey_vars(df: DataFrame, weight_var: str) -> None:
    """Validate that survey design variables exist and have no unexpected nulls."""
    required = ["VARSTR", "VARPSU", weight_var]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required survey design columns: {missing}")
