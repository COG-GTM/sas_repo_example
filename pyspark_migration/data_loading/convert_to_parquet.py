"""
Convert SAS transport file to Parquet format for optimal I/O performance.

This script performs a one-time conversion of the MEPS 2016 Full-Year
Consolidated file (H192) from SAS transport format (.ssp) to Parquet format.

Usage:
    python convert_to_parquet.py --input C:/MEPS/h192.ssp \\
        --output C:/MEPS/parquet/h192.parquet
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession


def initialize_spark(app_name="MEPS_SAS_to_Parquet"):
    """
    Initialize Spark session with optimized I/O settings for Parquet conversion.

    Returns:
        SparkSession: Configured Spark session
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.files.maxPartitionBytes", "128MB")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.autoBroadcastJoinThreshold", "10485760")
        .getOrCreate()
    )
    return spark


def convert_sas_to_parquet(input_path, output_path, spark=None):
    """
    Convert SAS transport file to Parquet format.

    Args:
        input_path (str): Path to input SAS transport file (.ssp)
        output_path (str): Path for output Parquet file
        spark (SparkSession, optional): Existing Spark session.
            If None, creates new one.

    Returns:
        bool: True if conversion successful, False otherwise
    """
    close_spark = False
    if spark is None:
        spark = initialize_spark()
        close_spark = True

    try:
        print(f"Reading SAS transport file from: {input_path}")
        sas_df = pd.read_sas(input_path, format="xport")

        print(f"Loaded {len(sas_df)} records with {len(sas_df.columns)} columns")
        print(f"Columns: {', '.join(sas_df.columns.tolist())}")

        print("Converting to Spark DataFrame...")
        spark_df = spark.createDataFrame(sas_df)

        print(f"Writing to Parquet format at: {output_path}")
        spark_df.write.mode("overwrite").parquet(output_path)

        print("Conversion completed successfully!")
        print(f"Output saved to: {output_path}")

        parquet_df = spark.read.parquet(output_path)
        record_count = parquet_df.count()
        print(f"Verification: Parquet file contains {record_count} records")

        return True

    except FileNotFoundError:
        print(f"Error: Input file not found: {input_path}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Error during conversion: {str(e)}", file=sys.stderr)
        return False
    finally:
        if close_spark:
            spark.stop()


def main():
    """Main entry point for command-line execution."""
    parser = argparse.ArgumentParser(
        description="Convert SAS transport file to Parquet format"
    )
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="Path to input SAS transport file (.ssp)",
    )
    parser.add_argument(
        "--output",
        "-o",
        required=True,
        help="Path for output Parquet file",
    )

    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: Input file does not exist: {input_path}", file=sys.stderr)
        sys.exit(1)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    success = convert_sas_to_parquet(str(input_path), str(output_path))
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
