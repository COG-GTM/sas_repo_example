"""Tests for Low Complexity PySpark Migration Jobs.

Covers:
  - Schema validation: output DataFrames have expected columns and data types
  - Row count assertions: row counts match input
  - Null/zero checks: no unexpected nulls in key columns
  - Variable creation correctness
"""

import pytest
from pyspark.sql.functions import col


# ---------------------------------------------------------------------------
# Exercise 1a Tests
# ---------------------------------------------------------------------------


class TestExercise1a:
    """Tests for Exercise 1a - National health care expenses, 2016."""

    def test_schema_has_expected_columns(self, spark, h192_data):
        """Output DataFrame must have all required columns."""
        from pyspark_migration.jobs.low_complexity.exercise_1a import build_etl_dataframe

        df = build_etl_dataframe(spark, h192_data)
        expected = {
            "DUPERSID", "TOTEXP16", "AGE16X", "AGE42X", "AGE31X",
            "VARSTR", "VARPSU", "PERWT16F",
            "TOTAL", "X_ANYSVCE", "AGE", "AGECAT",
        }
        assert expected.issubset(set(df.columns)), (
            f"Missing columns: {expected - set(df.columns)}"
        )

    def test_no_null_survey_design_vars(self, spark, h192_data):
        """VARSTR, VARPSU, PERWT16F must never be null."""
        from pyspark_migration.jobs.low_complexity.exercise_1a import build_etl_dataframe

        df = build_etl_dataframe(spark, h192_data)
        for c in ["DUPERSID", "VARSTR", "VARPSU", "PERWT16F"]:
            null_count = df.filter(col(c).isNull()).count()
            assert null_count == 0, f"{c} has {null_count} null values"

    def test_x_anysvce_binary(self, spark, h192_data):
        """X_ANYSVCE must be 0 or 1."""
        from pyspark_migration.jobs.low_complexity.exercise_1a import build_etl_dataframe

        df = build_etl_dataframe(spark, h192_data)
        invalid = df.filter(~col("X_ANYSVCE").isin([0, 1])).count()
        assert invalid == 0, f"X_ANYSVCE has {invalid} non-binary values"

    def test_x_anysvce_matches_totexp(self, spark, h192_data):
        """X_ANYSVCE=1 iff TOTEXP16 > 0."""
        from pyspark_migration.jobs.low_complexity.exercise_1a import build_etl_dataframe

        df = build_etl_dataframe(spark, h192_data)
        mismatch = df.filter(
            ((col("TOTEXP16") > 0) & (col("X_ANYSVCE") != 1))
            | ((col("TOTEXP16") == 0) & (col("X_ANYSVCE") != 0))
        ).count()
        assert mismatch == 0, f"{mismatch} rows where X_ANYSVCE doesn't match TOTEXP16"

    def test_agecat_values(self, spark, h192_data):
        """AGECAT must be 1 (0-64) or 2 (65+)."""
        from pyspark_migration.jobs.low_complexity.exercise_1a import build_etl_dataframe

        df = build_etl_dataframe(spark, h192_data)
        valid_vals = df.filter(col("AGECAT").isin([1, 2])).count()
        total = df.filter(col("AGECAT").isNotNull()).count()
        assert valid_vals == total

    def test_row_count_preserved(self, spark, h192_data):
        """Output row count must equal input row count."""
        from pyspark_migration.jobs.low_complexity.exercise_1a import build_etl_dataframe

        input_df = spark.read.parquet(h192_data)
        output_df = build_etl_dataframe(spark, h192_data)
        assert output_df.count() == input_df.count()


# ---------------------------------------------------------------------------
# Exercise 1b Tests
# ---------------------------------------------------------------------------


class TestExercise1b:
    """Tests for Exercise 1b - Expenses by type of service, 2015."""

    def test_schema_has_service_columns(self, spark, h181_data):
        """Output must have service category expenditure columns."""
        from pyspark_migration.jobs.low_complexity.exercise_1b import build_etl_dataframe

        df = build_etl_dataframe(spark, h181_data)
        expected = {
            "HOSPITAL_INPATIENT", "AMBULATORY", "PRESCRIBED_MEDICINES",
            "DENTAL", "HOME_HEALTH_OTHER", "TOTAL",
        }
        assert expected.issubset(set(df.columns))

    def test_service_flag_columns(self, spark, h181_data):
        """Output must have flag columns for each service type."""
        from pyspark_migration.jobs.low_complexity.exercise_1b import build_etl_dataframe

        df = build_etl_dataframe(spark, h181_data)
        expected = {
            "X_ANYSVCE", "X_HOSPITAL_INPATIENT", "X_AMBULATORY",
            "X_PRESCRIBED_MEDICINES", "X_DENTAL", "X_HOME_HEALTH_OTHER",
        }
        assert expected.issubset(set(df.columns))

    def test_service_flags_binary(self, spark, h181_data):
        """All service flag variables must be binary (0 or 1)."""
        from pyspark_migration.jobs.low_complexity.exercise_1b import build_etl_dataframe

        df = build_etl_dataframe(spark, h181_data)
        for flag in ["X_ANYSVCE", "X_HOSPITAL_INPATIENT", "X_AMBULATORY",
                      "X_PRESCRIBED_MEDICINES", "X_DENTAL", "X_HOME_HEALTH_OTHER"]:
            invalid = df.filter(~col(flag).isin([0, 1])).count()
            assert invalid == 0, f"{flag} has {invalid} non-binary values"

    def test_no_null_survey_vars(self, spark, h181_data):
        """Survey design variables must not be null."""
        from pyspark_migration.jobs.low_complexity.exercise_1b import build_etl_dataframe

        df = build_etl_dataframe(spark, h181_data)
        for c in ["DUPERSID", "VARSTR", "VARPSU", "PERWT15F"]:
            assert df.filter(col(c).isNull()).count() == 0


# ---------------------------------------------------------------------------
# Exercise 1c Tests
# ---------------------------------------------------------------------------


class TestExercise1c:
    """Tests for Exercise 1c - National health care expenses, 2018."""

    def test_schema_columns(self, spark, tmp_path_factory):
        """Output must have expected columns."""
        from pyspark_migration.jobs.low_complexity.exercise_1c import build_etl_dataframe

        path = str(tmp_path_factory.mktemp("h209"))
        data = [
            ("P001", 5000.0, 45, 100, 1, 1500.0, 22),
            ("P002", 0.0, 70, 200, 2, 2000.0, 23),
        ]
        from pyspark.sql.types import (
            DoubleType, IntegerType, StringType, StructField, StructType,
        )
        schema = StructType([
            StructField("DUPERSID", StringType()),
            StructField("TOTEXP18", DoubleType()),
            StructField("AGELAST", IntegerType()),
            StructField("VARSTR", IntegerType()),
            StructField("VARPSU", IntegerType()),
            StructField("PERWT18F", DoubleType()),
            StructField("PANEL", IntegerType()),
        ])
        spark.createDataFrame(data, schema).write.mode("overwrite").parquet(path)

        df = build_etl_dataframe(spark, path)
        expected = {"DUPERSID", "TOTEXP18", "VARSTR", "VARPSU", "PERWT18F",
                    "WITH_AN_EXPENSE", "CHAR_WITH_AN_EXPENSE", "AGECAT"}
        assert expected.issubset(set(df.columns))

    def test_with_an_expense_labels(self, spark, tmp_path_factory):
        """CHAR_WITH_AN_EXPENSE must be 'Any Expense' or 'No Expense'."""
        from pyspark_migration.jobs.low_complexity.exercise_1c import build_etl_dataframe

        path = str(tmp_path_factory.mktemp("h209_2"))
        from pyspark.sql.types import (
            DoubleType, IntegerType, StringType, StructField, StructType,
        )
        schema = StructType([
            StructField("DUPERSID", StringType()),
            StructField("TOTEXP18", DoubleType()),
            StructField("AGELAST", IntegerType()),
            StructField("VARSTR", IntegerType()),
            StructField("VARPSU", IntegerType()),
            StructField("PERWT18F", DoubleType()),
            StructField("PANEL", IntegerType()),
        ])
        data = [("P001", 5000.0, 45, 100, 1, 1500.0, 22),
                ("P002", 0.0, 70, 200, 2, 2000.0, 23)]
        spark.createDataFrame(data, schema).write.mode("overwrite").parquet(path)

        df = build_etl_dataframe(spark, path)
        valid_labels = {"Any Expense", "No Expense"}
        actual_labels = {row["CHAR_WITH_AN_EXPENSE"] for row in df.collect()}
        assert actual_labels.issubset(valid_labels)


# ---------------------------------------------------------------------------
# Care Access 2019 Tests
# ---------------------------------------------------------------------------


class TestCareAccess2019:
    """Tests for care_access_2019 - Access to care by poverty status."""

    def test_afford_variables_created(self, spark, h216_data):
        """Output must have afford_MD, afford_DN, afford_PM, afford_ANY."""
        from pyspark_migration.jobs.low_complexity.care_access_2019 import build_etl_dataframe

        df = build_etl_dataframe(spark, h216_data)
        expected = {"afford_MD", "afford_DN", "afford_PM", "afford_ANY", "domain"}
        assert expected.issubset(set(df.columns))

    def test_afford_variables_binary(self, spark, h216_data):
        """Afford variables must be 0 or 1."""
        from pyspark_migration.jobs.low_complexity.care_access_2019 import build_etl_dataframe

        df = build_etl_dataframe(spark, h216_data)
        for c in ["afford_MD", "afford_DN", "afford_PM", "afford_ANY", "domain"]:
            invalid = df.filter(~col(c).isin([0, 1])).count()
            assert invalid == 0, f"{c} has {invalid} non-binary values"

    def test_weight_adjustment_for_zero_weight_nondomain(self, spark, h216_data):
        """domain=0 and PERWT19F=0 records should have PERWT19F set to 1."""
        from pyspark_migration.jobs.low_complexity.care_access_2019 import build_etl_dataframe

        df = build_etl_dataframe(spark, h216_data)
        # No records should have domain=0 AND PERWT19F=0
        violating = df.filter(
            (col("domain") == 0) & (col("PERWT19F") == 0)
        ).count()
        assert violating == 0, "Weight adjustment failed"


# ---------------------------------------------------------------------------
# Insurance by Age 2016 Tests
# ---------------------------------------------------------------------------


class TestInsAge2016:
    """Tests for ins_age_2016 - Health insurance by age group."""

    def test_age_group_created(self, spark, h192_data):
        """Output must have AGE_GRP column."""
        from pyspark_migration.jobs.low_complexity.ins_age_2016 import build_etl_dataframe

        df = build_etl_dataframe(spark, h192_data)
        assert "AGE_GRP" in df.columns

    def test_insurance_category_created(self, spark, h192_data):
        """Output must have INS_CAT column."""
        from pyspark_migration.jobs.low_complexity.ins_age_2016 import build_etl_dataframe

        df = build_etl_dataframe(spark, h192_data)
        assert "INS_CAT" in df.columns

    def test_survey_design_vars_present(self, spark, h192_data):
        """Survey design variables must be present."""
        from pyspark_migration.jobs.low_complexity.ins_age_2016 import build_etl_dataframe

        df = build_etl_dataframe(spark, h192_data)
        for c in ["VARSTR", "VARPSU", "PERWT16F"]:
            assert c in df.columns
            assert df.filter(col(c).isNull()).count() == 0
