"""
Unit tests for low-complexity PySpark ETL jobs.

Tests cover:
- Schema validation: output has expected columns and data types
- Row count assertions
- Null/zero checks on key columns
- Correct variable derivation
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, LongType
)
from pyspark.sql.functions import col

from pyspark_migration.etl.low_complexity.exercise_1a import run_etl as exercise_1a_etl
from pyspark_migration.etl.low_complexity.exercise_1b import run_etl as exercise_1b_etl
from pyspark_migration.etl.low_complexity.exercise_1c import run_etl as exercise_1c_etl
from pyspark_migration.etl.low_complexity.care_access_2019 import run_etl as care_access_etl
from pyspark_migration.etl.low_complexity.ins_age_2016 import run_etl as ins_age_etl
from pyspark_migration.etl.low_complexity.use_expenditures_2016 import run_etl as use_exp_etl


# ---------------------------------------------------------------------------
# Exercise 1a tests
# ---------------------------------------------------------------------------

class TestExercise1a:
    """Tests for Exercise1a: National health care expenses, 2016."""

    def test_schema_has_required_columns(self, spark):
        data = [("P001", 500.0, 45, 45, 45, "S1", "P1", 1500.0)]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "TOTEXP16", "AGE16X", "AGE42X", "AGE31X",
             "VARSTR", "VARPSU", "PERWT16F"]
        )
        result = exercise_1a_etl(spark, df)
        expected_cols = {"DUPERSID", "TOTAL", "X_ANYSVCE", "AGE", "AGECAT",
                         "VARSTR", "VARPSU", "PERWT16F"}
        assert expected_cols.issubset(set(result.columns))

    def test_x_anysvce_flag_zero_expense(self, spark):
        data = [("P001", 0.0, 30, 30, 30, "S1", "P1", 1000.0)]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "TOTEXP16", "AGE16X", "AGE42X", "AGE31X",
             "VARSTR", "VARPSU", "PERWT16F"]
        )
        result = exercise_1a_etl(spark, df)
        row = result.collect()[0]
        assert row["X_ANYSVCE"] == 0

    def test_x_anysvce_flag_positive_expense(self, spark):
        data = [("P001", 250.0, 30, 30, 30, "S1", "P1", 1000.0)]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "TOTEXP16", "AGE16X", "AGE42X", "AGE31X",
             "VARSTR", "VARPSU", "PERWT16F"]
        )
        result = exercise_1a_etl(spark, df)
        row = result.collect()[0]
        assert row["X_ANYSVCE"] == 1

    def test_age_derivation_from_age16x(self, spark):
        data = [("P001", 100.0, 50, -1, -1, "S1", "P1", 1000.0)]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "TOTEXP16", "AGE16X", "AGE42X", "AGE31X",
             "VARSTR", "VARPSU", "PERWT16F"]
        )
        result = exercise_1a_etl(spark, df)
        row = result.collect()[0]
        assert row["AGE"] == 50

    def test_age_derivation_fallback_to_age42x(self, spark):
        data = [("P001", 100.0, -1, 42, -1, "S1", "P1", 1000.0)]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "TOTEXP16", "AGE16X", "AGE42X", "AGE31X",
             "VARSTR", "VARPSU", "PERWT16F"]
        )
        result = exercise_1a_etl(spark, df)
        row = result.collect()[0]
        assert row["AGE"] == 42

    def test_agecat_under_65(self, spark):
        data = [("P001", 100.0, 30, 30, 30, "S1", "P1", 1000.0)]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "TOTEXP16", "AGE16X", "AGE42X", "AGE31X",
             "VARSTR", "VARPSU", "PERWT16F"]
        )
        result = exercise_1a_etl(spark, df)
        row = result.collect()[0]
        assert row["AGECAT"] == 1

    def test_agecat_65_plus(self, spark):
        data = [("P001", 100.0, 70, 70, 70, "S1", "P1", 1000.0)]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "TOTEXP16", "AGE16X", "AGE42X", "AGE31X",
             "VARSTR", "VARPSU", "PERWT16F"]
        )
        result = exercise_1a_etl(spark, df)
        row = result.collect()[0]
        assert row["AGECAT"] == 2

    def test_row_count_preserved(self, spark):
        data = [
            ("P001", 100.0, 30, 30, 30, "S1", "P1", 1000.0),
            ("P002", 0.0, 70, 70, 70, "S1", "P1", 2000.0),
            ("P003", 500.0, 20, 20, 20, "S1", "P1", 1500.0),
        ]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "TOTEXP16", "AGE16X", "AGE42X", "AGE31X",
             "VARSTR", "VARPSU", "PERWT16F"]
        )
        result = exercise_1a_etl(spark, df)
        assert result.count() == 3

    def test_no_null_survey_vars(self, spark):
        data = [("P001", 100.0, 30, 30, 30, "S1", "P1", 1000.0)]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "TOTEXP16", "AGE16X", "AGE42X", "AGE31X",
             "VARSTR", "VARPSU", "PERWT16F"]
        )
        result = exercise_1a_etl(spark, df)
        null_count = result.filter(
            col("VARSTR").isNull() | col("VARPSU").isNull() | col("PERWT16F").isNull()
        ).count()
        assert null_count == 0


# ---------------------------------------------------------------------------
# Exercise 1b tests
# ---------------------------------------------------------------------------

class TestExercise1b:
    """Tests for Exercise1b: Health care expenses by type of service, 2015."""

    def test_service_type_columns_exist(self, spark):
        data = [("P001", 1000.0, 200.0, 100.0, 300.0, 150.0, 50.0, 25.0,
                 100.0, 30.0, 20.0, 10.0, 5.0, 3.0, 7.0,
                 40, 40, 40, "S1", "P1", 1500.0)]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "TOTEXP15", "IPDEXP15", "IPFEXP15", "OBVEXP15",
             "RXEXP15", "OPDEXP15", "OPFEXP15", "DVTEXP15", "ERDEXP15",
             "ERFEXP15", "HHAEXP15", "HHNEXP15", "OTHEXP15", "VISEXP15",
             "AGE15X", "AGE42X", "AGE31X", "VARSTR", "VARPSU", "PERWT15F"]
        )
        result = exercise_1b_etl(spark, df)
        expected = {"HOSPITAL_INPATIENT", "AMBULATORY", "PRESCRIBED_MEDICINES",
                    "DENTAL", "HOME_HEALTH_OTHER", "X_ANYSVCE"}
        assert expected.issubset(set(result.columns))

    def test_hospital_inpatient_calculation(self, spark):
        data = [("P001", 1000.0, 200.0, 100.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                 40, 40, 40, "S1", "P1", 1500.0)]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "TOTEXP15", "IPDEXP15", "IPFEXP15", "OBVEXP15",
             "RXEXP15", "OPDEXP15", "OPFEXP15", "DVTEXP15", "ERDEXP15",
             "ERFEXP15", "HHAEXP15", "HHNEXP15", "OTHEXP15", "VISEXP15",
             "AGE15X", "AGE42X", "AGE31X", "VARSTR", "VARPSU", "PERWT15F"]
        )
        result = exercise_1b_etl(spark, df)
        row = result.collect()[0]
        assert row["HOSPITAL_INPATIENT"] == 300.0  # IPDEXP + IPFEXP

    def test_flags_for_zero_expense(self, spark):
        data = [("P001", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                 40, 40, 40, "S1", "P1", 1500.0)]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "TOTEXP15", "IPDEXP15", "IPFEXP15", "OBVEXP15",
             "RXEXP15", "OPDEXP15", "OPFEXP15", "DVTEXP15", "ERDEXP15",
             "ERFEXP15", "HHAEXP15", "HHNEXP15", "OTHEXP15", "VISEXP15",
             "AGE15X", "AGE42X", "AGE31X", "VARSTR", "VARPSU", "PERWT15F"]
        )
        result = exercise_1b_etl(spark, df)
        row = result.collect()[0]
        assert row["X_ANYSVCE"] == 0
        assert row["X_HOSPITAL_INPATIENT"] == 0


# ---------------------------------------------------------------------------
# Exercise 1c tests
# ---------------------------------------------------------------------------

class TestExercise1c:
    """Tests for Exercise1c: National health care expenses, 2018."""

    def test_expense_categorization(self, spark):
        data = [
            ("P001", 500.0, 45, "S1", "P1", 1500.0),
            ("P002", 0.0, 70, "S1", "P1", 2000.0),
        ]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "TOTEXP18", "AGELAST", "VARSTR", "VARPSU", "PERWT18F"]
        )
        result = exercise_1c_etl(spark, df)
        rows = {r["DUPERSID"]: r for r in result.collect()}
        assert rows["P001"]["CHAR_WITH_AN_EXPENSE"] == "Any Expense"
        assert rows["P002"]["CHAR_WITH_AN_EXPENSE"] == "No Expense"

    def test_agecat_assignment(self, spark):
        data = [
            ("P001", 100.0, 30, "S1", "P1", 1000.0),
            ("P002", 100.0, 70, "S1", "P1", 1000.0),
        ]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "TOTEXP18", "AGELAST", "VARSTR", "VARPSU", "PERWT18F"]
        )
        result = exercise_1c_etl(spark, df)
        rows = {r["DUPERSID"]: r for r in result.collect()}
        assert rows["P001"]["AGECAT"] == "0-64"
        assert rows["P002"]["AGECAT"] == "65+"


# ---------------------------------------------------------------------------
# Care Access 2019 tests
# ---------------------------------------------------------------------------

class TestCareAccess2019:
    """Tests for care_access_2019: Affordability of care, 2019."""

    def test_afford_flags(self, spark):
        data = [("P001", 1, 2, 1, 1, 3, "S1", "P1", 1500.0)]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "AFRDCA42", "AFRDDN42", "AFRDPM42",
             "ACCELI42", "POVCAT19", "VARSTR", "VARPSU", "PERWT19F"]
        )
        result = care_access_etl(spark, df)
        row = result.collect()[0]
        assert row["afford_MD"] == 1
        assert row["afford_DN"] == 0
        assert row["afford_PM"] == 1
        assert row["afford_ANY"] == 1

    def test_domain_flag(self, spark):
        data = [
            ("P001", 2, 2, 2, 1, 3, "S1", "P1", 1500.0),
            ("P002", 2, 2, 2, 0, 3, "S1", "P1", 1500.0),
        ]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "AFRDCA42", "AFRDDN42", "AFRDPM42",
             "ACCELI42", "POVCAT19", "VARSTR", "VARPSU", "PERWT19F"]
        )
        result = care_access_etl(spark, df)
        rows = {r["DUPERSID"]: r for r in result.collect()}
        assert rows["P001"]["domain"] == 1
        assert rows["P002"]["domain"] == 0

    def test_weight_adjustment_for_zero_weight_non_domain(self, spark):
        data = [("P001", 2, 2, 2, 0, 3, "S1", "P1", 0.0)]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "AFRDCA42", "AFRDDN42", "AFRDPM42",
             "ACCELI42", "POVCAT19", "VARSTR", "VARPSU", "PERWT19F"]
        )
        result = care_access_etl(spark, df)
        row = result.collect()[0]
        assert row["PERWT19F"] == 1.0  # Adjusted from 0 to 1


# ---------------------------------------------------------------------------
# Insurance by Age 2016 tests
# ---------------------------------------------------------------------------

class TestInsAge2016:
    """Tests for ins_age_2016: Health insurance by age group, 2016."""

    def test_age_groups(self, spark):
        data = [
            ("P001", 3, 1, "S1", "P1", 1000.0),
            ("P002", 10, 2, "S1", "P1", 1000.0),
            ("P003", 30, 1, "S1", "P1", 1000.0),
            ("P004", 50, 3, "S1", "P1", 1000.0),
            ("P005", 70, 4, "S1", "P1", 1000.0),
        ]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "AGELAST", "INSURC16", "VARSTR", "VARPSU", "PERWT16F"]
        )
        result = ins_age_etl(spark, df)
        rows = {r["DUPERSID"]: r for r in result.collect()}
        assert rows["P001"]["AGE_GRP"] == "Under 5"
        assert rows["P002"]["AGE_GRP"] == "5-17"
        assert rows["P003"]["AGE_GRP"] == "18-44"
        assert rows["P004"]["AGE_GRP"] == "45-64"
        assert rows["P005"]["AGE_GRP"] == "65+"

    def test_insurance_categories(self, spark):
        data = [
            ("P001", 30, 1, "S1", "P1", 1000.0),
            ("P002", 30, 3, "S1", "P1", 1000.0),
            ("P003", 70, 4, "S1", "P1", 1000.0),
        ]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "AGELAST", "INSURC16", "VARSTR", "VARPSU", "PERWT16F"]
        )
        result = ins_age_etl(spark, df)
        rows = {r["DUPERSID"]: r for r in result.collect()}
        assert rows["P001"]["INS_CATEGORY"] == "<65, Any private"
        assert rows["P002"]["INS_CATEGORY"] == "<65, Uninsured"
        assert rows["P003"]["INS_CATEGORY"] == "65+, Medicare only"
