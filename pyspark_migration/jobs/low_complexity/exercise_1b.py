"""PySpark Migration: Exercise 1b - National Health Care Expenses by Type of Service, 2015.

Original: SAS/workshop_exercises/exercise_1b/Exercise1b.sas

Generates estimates on national health care expenses by type of service, 2015:
  (1) Percentage distribution of expenses by type of service
  (2) Percentage of persons with an expense, by type of service
  (3) Mean expense per person with an expense, by type of service

Service categories:
  - Hospital Inpatient
  - Ambulatory (Office-Based + Hospital Outpatient + ER)
  - Prescribed Medicines
  - Dental
  - Home Health/Other

Input: H181 (2015 Full-Year Consolidated file)
Survey design: VARSTR (strata), VARPSU (cluster), PERWT15F (weight)
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, when


def run_etl(spark: SparkSession, input_path: str, output_path: str) -> DataFrame:
    """Execute the Exercise 1b ETL pipeline.

    Args:
        spark: Active SparkSession.
        input_path: Path to H181 data file.
        output_path: Path to write output Parquet.

    Returns:
        Transformed DataFrame ready for survey estimation.
    """
    df = build_etl_dataframe(spark, input_path)
    df.write.mode("overwrite").parquet(output_path)
    return df


def build_etl_dataframe(spark: SparkSession, input_path: str) -> DataFrame:
    """Build the ETL DataFrame without writing output.

    Args:
        spark: Active SparkSession.
        input_path: Path to H181 data file.

    Returns:
        Transformed DataFrame.
    """
    h181 = spark.read.parquet(input_path)

    # Select needed variables
    h181 = h181.select(
        "DUPERSID", "TOTEXP15", "IPDEXP15", "IPFEXP15", "OBVEXP15", "RXEXP15",
        "OPDEXP15", "OPFEXP15", "DVTEXP15", "ERDEXP15", "ERFEXP15",
        "HHAEXP15", "HHNEXP15", "OTHEXP15", "VISEXP15",
        "AGE15X", "AGE42X", "AGE31X",
        "VARSTR", "VARPSU", "PERWT15F"
    )

    # Define expenditure variables by type of service
    h181 = h181.withColumn("TOTAL", col("TOTEXP15"))
    h181 = h181.withColumn(
        "HOSPITAL_INPATIENT", col("IPDEXP15") + col("IPFEXP15")
    )
    h181 = h181.withColumn(
        "AMBULATORY",
        col("OBVEXP15") + col("OPDEXP15") + col("OPFEXP15")
        + col("ERDEXP15") + col("ERFEXP15")
    )
    h181 = h181.withColumn("PRESCRIBED_MEDICINES", col("RXEXP15"))
    h181 = h181.withColumn("DENTAL", col("DVTEXP15"))
    h181 = h181.withColumn(
        "HOME_HEALTH_OTHER",
        col("HHAEXP15") + col("HHNEXP15") + col("OTHEXP15") + col("VISEXP15")
    )

    # QC: Difference should be zero
    h181 = h181.withColumn(
        "DIFF",
        col("TOTAL") - col("HOSPITAL_INPATIENT") - col("AMBULATORY")
        - col("PRESCRIBED_MEDICINES") - col("DENTAL") - col("HOME_HEALTH_OTHER")
    )

    # Create flag variables for persons with an expense by type of service
    expense_cols = [
        ("TOTAL", "X_ANYSVCE"),
        ("HOSPITAL_INPATIENT", "X_HOSPITAL_INPATIENT"),
        ("AMBULATORY", "X_AMBULATORY"),
        ("PRESCRIBED_MEDICINES", "X_PRESCRIBED_MEDICINES"),
        ("DENTAL", "X_DENTAL"),
        ("HOME_HEALTH_OTHER", "X_HOME_HEALTH_OTHER"),
    ]

    for exp_col, flag_col in expense_cols:
        h181 = h181.withColumn(
            flag_col,
            when(col(exp_col) > 0, 1).otherwise(0)
        )

    # Create age variable from end-of-year, round 4/2, round 3/1
    h181 = h181.withColumn(
        "AGE",
        when(col("AGE15X") >= 0, col("AGE15X"))
        .when(col("AGE42X") >= 0, col("AGE42X"))
        .when(col("AGE31X") >= 0, col("AGE31X"))
    )

    # Create age category: 1 = 0-64, 2 = 65+
    h181 = h181.withColumn(
        "AGECAT",
        when((col("AGE") >= 0) & (col("AGE") <= 64), lit(1))
        .when(col("AGE") > 64, lit(2))
    )

    return h181
