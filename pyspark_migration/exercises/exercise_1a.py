"""
Exercise 1a: National Health Care Expenses Analysis (2016)

This script replicates the SAS Exercise 1a analysis using PySpark with Parquet I/O.
It generates estimates on national health care expenses for 2016:
    1. Overall expenses
    2. Percentage of persons with an expense
    3. Mean expense per person with an expense

The analysis is performed overall and by age group (0-64 and 65+).
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def create_derived_variables(df: DataFrame) -> DataFrame:
    """
    Create derived variables matching SAS logic.

    This function creates:
    - TOTAL: Copy of TOTEXP16
    - X_ANYSVCE: Binary flag (1 if any expense, 0 otherwise)
    - AGE: Consolidated age from AGE16X, AGE42X, AGE31X
    - age_group: Categorical age group ('0-64', '65+', 'ALL AGES')
    - age_category: Numeric age category (1='0-64', 2='65+')

    Args:
        df (DataFrame): Input DataFrame with raw MEPS data

    Returns:
        DataFrame: DataFrame with derived variables added
    """
    df = df.withColumn("TOTAL", F.col("TOTEXP16"))

    df = df.withColumn("X_ANYSVCE", F.when(F.col("TOTAL") > 0, 1).otherwise(0))

    df = df.withColumn(
        "AGE",
        F.when(F.col("AGE16X") >= 0, F.col("AGE16X"))
        .when(F.col("AGE42X") >= 0, F.col("AGE42X"))
        .when(F.col("AGE31X") >= 0, F.col("AGE31X"))
        .otherwise(None),
    )

    df = df.withColumn(
        "age_group",
        F.when(F.col("AGE") < 65, "0-64")
        .when(F.col("AGE") >= 65, "65+")
        .otherwise("ALL AGES"),
    )

    df = df.withColumn(
        "age_category",
        F.when((F.col("AGE") >= 0) & (F.col("AGE") <= 64), 1)
        .when(F.col("AGE") > 64, 2)
        .otherwise(None),
    )

    return df


def calculate_overall_statistics(df: DataFrame) -> DataFrame:
    """
    Calculate overall statistics (not stratified by age group).

    Calculates:
    - Total population (sum of weights)
    - Population with any expense
    - Total expenses
    - Percentage with expense
    - Mean expense per person with expense
    - Sample size

    Args:
        df (DataFrame): Input DataFrame with derived variables

    Returns:
        DataFrame: Overall statistics
    """
    overall_stats = df.agg(
        F.sum("PERWT16F").alias("total_pop"),
        F.sum(F.when(F.col("X_ANYSVCE") == 1, F.col("PERWT16F")).otherwise(0)).alias(
            "pop_with_exp"
        ),
        F.sum(F.col("TOTAL") * F.col("PERWT16F")).alias("total_exp"),
        F.count("*").alias("sample_size"),
    )

    overall_stats = overall_stats.withColumn(
        "pct_with_expense", (F.col("pop_with_exp") / F.col("total_pop")) * 100
    )

    overall_stats = overall_stats.withColumn(
        "mean_exp_per_person_with_exp", F.col("total_exp") / F.col("pop_with_exp")
    )

    overall_stats = overall_stats.withColumn("age_group", F.lit("ALL AGES"))

    return overall_stats


def calculate_age_group_statistics(df: DataFrame) -> DataFrame:
    """
    Calculate statistics by age group.

    Calculates the same statistics as calculate_overall_statistics,
    but stratified by age group (0-64 and 65+).

    Args:
        df (DataFrame): Input DataFrame with derived variables

    Returns:
        DataFrame: Statistics by age group
    """
    age_stats = df.groupBy("age_group").agg(
        F.sum("PERWT16F").alias("total_pop"),
        F.sum(F.when(F.col("X_ANYSVCE") == 1, F.col("PERWT16F")).otherwise(0)).alias(
            "pop_with_exp"
        ),
        F.sum(F.col("TOTAL") * F.col("PERWT16F")).alias("total_exp"),
        F.count("*").alias("sample_size"),
    )

    age_stats = age_stats.withColumn(
        "pct_with_expense", (F.col("pop_with_exp") / F.col("total_pop")) * 100
    )

    age_stats = age_stats.withColumn(
        "mean_exp_per_person_with_exp", F.col("total_exp") / F.col("pop_with_exp")
    )

    return age_stats


def calculate_variance_components(df: DataFrame) -> DataFrame:
    """
    Calculate variance components for survey design.

    This function calculates PSU-level and stratum-level statistics
    needed for proper variance estimation with complex survey designs.

    The variance estimation follows the Taylor series linearization method
    used by SAS PROC SURVEYMEANS.

    Args:
        df (DataFrame): Input DataFrame with survey design variables

    Returns:
        DataFrame: Variance components by stratum, PSU, and age group
    """
    psu_stats = df.groupBy("VARSTR", "VARPSU", "age_group").agg(
        F.sum(F.col("TOTAL") * F.col("PERWT16F")).alias("psu_total_exp"),
        F.sum(F.when(F.col("X_ANYSVCE") == 1, F.col("PERWT16F")).otherwise(0)).alias(
            "psu_pop_with_exp"
        ),
        F.sum("PERWT16F").alias("psu_weight"),
        F.count("*").alias("psu_sample_size"),
    )

    psu_stats = psu_stats.withColumn(
        "psu_mean_exp",
        F.when(
            F.col("psu_pop_with_exp") > 0,
            F.col("psu_total_exp") / F.col("psu_pop_with_exp"),
        ).otherwise(0),
    )

    stratum_stats = psu_stats.groupBy("VARSTR", "age_group").agg(
        F.count("VARPSU").alias("n_psu"),
        F.mean("psu_total_exp").alias("mean_psu_total_exp"),
        F.variance("psu_total_exp").alias("var_psu_total_exp"),
        F.mean("psu_mean_exp").alias("mean_psu_mean_exp"),
        F.variance("psu_mean_exp").alias("var_psu_mean_exp"),
        F.sum("psu_weight").alias("stratum_weight"),
    )

    stratum_stats = stratum_stats.withColumn(
        "se_total_exp",
        F.sqrt(F.col("var_psu_total_exp") * F.col("n_psu") / (F.col("n_psu") - 1)),
    )

    stratum_stats = stratum_stats.withColumn(
        "se_mean_exp",
        F.sqrt(F.col("var_psu_mean_exp") * F.col("n_psu") / (F.col("n_psu") - 1)),
    )

    return stratum_stats


def calculate_combined_variance(variance_df: DataFrame) -> DataFrame:
    """
    Calculate combined variance estimates across all strata.

    This aggregates the stratum-level variance components to produce
    overall standard errors for each age group.

    Args:
        variance_df (DataFrame): Stratum-level variance components

    Returns:
        DataFrame: Combined variance estimates by age group
    """
    combined_variance = variance_df.groupBy("age_group").agg(
        F.sum(F.pow(F.col("se_total_exp"), 2)).alias("total_variance_exp"),
        F.sum(F.pow(F.col("se_mean_exp"), 2)).alias("total_variance_mean_exp"),
        F.sum("stratum_weight").alias("total_weight"),
        F.sum("n_psu").alias("total_psu"),
    )

    combined_variance = combined_variance.withColumn(
        "se_total_exp", F.sqrt(F.col("total_variance_exp"))
    )

    combined_variance = combined_variance.withColumn(
        "se_mean_exp", F.sqrt(F.col("total_variance_mean_exp"))
    )

    return combined_variance


def exercise_1a_etl(
    input_path: str,
    output_path: str,
    variance_output_path: Optional[str] = None,
    spark: Optional[SparkSession] = None,
) -> tuple:
    """
    Complete ETL pipeline for Exercise 1a with Parquet I/O.

    This function:
    1. Reads MEPS data from Parquet with column pruning
    2. Creates derived variables
    3. Calculates overall and age-group statistics
    4. Calculates variance components for survey design
    5. Writes results to Parquet

    Args:
        input_path (str): Path to source Parquet file
        output_path (str): Path for output results
        variance_output_path (str, optional): Path for variance components output
        spark (SparkSession, optional): Existing Spark session

    Returns:
        tuple: (results_df, variance_df) - Results and variance DataFrames
    """
    if spark is None:
        from pyspark_migration.data_loading.load_meps import get_spark_session

        spark = get_spark_session("MEPS_Exercise1a")

    print(f"Reading data from: {input_path}")
    df = spark.read.parquet(input_path).select(
        "DUPERSID",
        "TOTEXP16",
        "AGE16X",
        "AGE42X",
        "AGE31X",
        "VARSTR",
        "VARPSU",
        "PERWT16F",
    )

    print(f"Loaded {df.count()} records")

    print("Creating derived variables...")
    df = create_derived_variables(df)

    print("Calculating overall statistics...")
    overall_stats = calculate_overall_statistics(df)

    print("Calculating age group statistics...")
    age_stats = calculate_age_group_statistics(df)

    results = overall_stats.union(age_stats)

    results = results.select(
        "age_group",
        "sample_size",
        "total_pop",
        "pop_with_exp",
        "pct_with_expense",
        "total_exp",
        "mean_exp_per_person_with_exp",
    )

    print(f"Writing results to: {output_path}")
    results.write.mode("overwrite").parquet(output_path)

    print("Calculating variance components...")
    variance_components = calculate_variance_components(df)

    if variance_output_path:
        print(f"Writing variance components to: {variance_output_path}")
        variance_components.write.mode("overwrite").parquet(variance_output_path)

    print("\n=== RESULTS ===")
    results.orderBy("age_group").show(truncate=False)

    print("\n=== VARIANCE COMPONENTS (Sample) ===")
    combined_var = calculate_combined_variance(variance_components)
    combined_var.show(truncate=False)

    return results, variance_components


def main():
    """Main entry point for command-line execution."""
    parser = argparse.ArgumentParser(
        description="Exercise 1a: National Health Care Expenses Analysis"
    )
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="Path to input Parquet file (H192)",
    )
    parser.add_argument(
        "--output",
        "-o",
        required=True,
        help="Path for output results",
    )
    parser.add_argument(
        "--variance-output",
        "-v",
        help="Path for variance components output (optional)",
    )

    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: Input file does not exist: {input_path}", file=sys.stderr)
        sys.exit(1)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    variance_output_path = None
    if args.variance_output:
        variance_output_path = Path(args.variance_output)
        variance_output_path.parent.mkdir(parents=True, exist_ok=True)
        variance_output_path = str(variance_output_path)

    try:
        exercise_1a_etl(str(input_path), str(output_path), variance_output_path)
        print("\nAnalysis completed successfully!")
    except Exception as e:
        print(f"Error during analysis: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
