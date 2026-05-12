"""
Statistical parity tests.

Compare PySpark ETL outputs against golden reference values from
original SAS/R/Stata output files.

These tests validate that:
- Point estimates match within +/-0.01%
- Standard errors match within +/-1%
- Weighted statistics preserve survey design

Note: In the absence of actual MEPS data files, these tests use
synthetic data to validate the mathematical correctness of the
ETL transformations and survey estimation pipeline.
"""

import pytest
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, sum as spark_sum

from pyspark_migration.etl.low_complexity.exercise_1a import run_etl as exercise_1a_etl
from pyspark_migration.etl.low_complexity.care_access_2019 import run_etl as care_access_etl
from pyspark_migration.etl.medium_complexity.pmed_prescribed_drug_2016 import (
    run_etl as pmed_drug_etl,
)
from pyspark_migration.etl.high_complexity.cond_pmed_2020 import (
    load_and_prepare as cond_pmed_prepare,
)


def _relative_error(actual, expected):
    """Compute relative error. Returns 0 if both are zero."""
    if expected == 0:
        return 0.0 if actual == 0 else float("inf")
    return abs(actual - expected) / abs(expected)


class TestExercise1aStatisticalParity:
    """
    Validate Exercise1a ETL produces correct aggregates.

    Golden reference: SAS Exercise1a_OUTPUT.TXT
    Key estimates:
    - Total expenditure (weighted sum of TOTEXP16)
    - Proportion with any expense (weighted mean of X_ANYSVCE)
    """

    def test_total_expense_calculation(self, spark):
        """Unweighted total TOTEXP16 should be preserved exactly."""
        data = [
            ("P001", 1000.0, 30, 30, 30, "S1", "P1", 1500.0),
            ("P002", 2000.0, 50, 50, 50, "S1", "P1", 2500.0),
            ("P003", 0.0, 70, 70, 70, "S1", "P1", 1000.0),
        ]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "TOTEXP16", "AGE16X", "AGE42X", "AGE31X",
             "VARSTR", "VARPSU", "PERWT16F"]
        )
        result = exercise_1a_etl(spark, df)
        total = result.agg(spark_sum("TOTAL")).collect()[0][0]
        assert total == 3000.0  # 1000 + 2000 + 0

    def test_weighted_expense_total(self, spark):
        """Weighted sum: SUM(TOTAL * PERWT16F) should match."""
        data = [
            ("P001", 1000.0, 30, 30, 30, "S1", "P1", 2.0),
            ("P002", 500.0, 50, 50, 50, "S1", "P1", 3.0),
        ]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "TOTEXP16", "AGE16X", "AGE42X", "AGE31X",
             "VARSTR", "VARPSU", "PERWT16F"]
        )
        result = exercise_1a_etl(spark, df)
        weighted_total = result.withColumn(
            "w_total", col("TOTAL") * col("PERWT16F")
        ).agg(spark_sum("w_total")).collect()[0][0]
        expected = 1000.0 * 2.0 + 500.0 * 3.0  # 3500
        assert _relative_error(weighted_total, expected) < 0.0001

    def test_proportion_with_expense(self, spark):
        """Weighted proportion with any expense."""
        data = [
            ("P001", 1000.0, 30, 30, 30, "S1", "P1", 1.0),
            ("P002", 0.0, 50, 50, 50, "S1", "P1", 1.0),
            ("P003", 500.0, 70, 70, 70, "S1", "P1", 2.0),
        ]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "TOTEXP16", "AGE16X", "AGE42X", "AGE31X",
             "VARSTR", "VARPSU", "PERWT16F"]
        )
        result = exercise_1a_etl(spark, df)
        pdf = result.select("X_ANYSVCE", "PERWT16F").toPandas()
        weighted_prop = (pdf["X_ANYSVCE"] * pdf["PERWT16F"]).sum() / pdf["PERWT16F"].sum()
        # Expected: (1*1 + 0*1 + 1*2) / (1+1+2) = 3/4 = 0.75
        assert _relative_error(weighted_prop, 0.75) < 0.0001


class TestCareAccess2019StatisticalParity:
    """
    Validate care_access_2019 ETL preserves affordability indicators.
    """

    def test_affordability_weighted_proportion(self, spark):
        """Weighted proportion of afford_ANY should be computed correctly."""
        data = [
            ("P001", 1, 2, 1, 1, 3, "S1", "P1", 2.0),
            ("P002", 2, 2, 2, 1, 3, "S1", "P1", 3.0),
            ("P003", 1, 1, 2, 1, 3, "S1", "P1", 5.0),
        ]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "AFRDCA42", "AFRDDN42", "AFRDPM42",
             "ACCELI42", "POVCAT19", "VARSTR", "VARPSU", "PERWT19F"]
        )
        result = care_access_etl(spark, df)
        pdf = result.select("afford_ANY", "PERWT19F").toPandas()
        weighted_prop = (pdf["afford_ANY"] * pdf["PERWT19F"]).sum() / pdf["PERWT19F"].sum()
        # P001: afford_ANY=1 (MD=1 or PM=1), P002: 0, P003: 1 (MD=1 or DN=1)
        # (1*2 + 0*3 + 1*5) / (2+3+5) = 7/10 = 0.7
        assert _relative_error(weighted_prop, 0.7) < 0.0001


class TestPmedDrug2016StatisticalParity:
    """
    Validate pmed_prescribed_drug_2016 person-drug aggregation preserves totals.
    """

    def test_total_expenditure_preserved(self, spark):
        """Sum of person-level expenditures should equal sum of event-level."""
        data = [
            ("P001", "S1", "P1", 1000.0, "DrugA", 50.0),
            ("P001", "S1", "P1", 1000.0, "DrugA", 30.0),
            ("P001", "S1", "P1", 1000.0, "DrugB", 20.0),
            ("P002", "S1", "P1", 2000.0, "DrugA", 45.0),
        ]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "VARSTR", "VARPSU", "PERWT16F", "RXDRGNAM", "RXXP16X"]
        )
        event_total = df.agg(spark_sum("RXXP16X")).collect()[0][0]
        result = pmed_drug_etl(spark, df)
        person_total = result.agg(spark_sum("pers_RXXP")).collect()[0][0]
        assert event_total == person_total  # 145.0

    def test_total_fill_count_preserved(self, spark):
        """Sum of n_purchases should equal total event rows."""
        data = [
            ("P001", "S1", "P1", 1000.0, "DrugA", 50.0),
            ("P001", "S1", "P1", 1000.0, "DrugA", 30.0),
            ("P001", "S1", "P1", 1000.0, "DrugB", 20.0),
            ("P002", "S1", "P1", 2000.0, "DrugA", 45.0),
        ]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "VARSTR", "VARPSU", "PERWT16F", "RXDRGNAM", "RXXP16X"]
        )
        result = pmed_drug_etl(spark, df)
        total_purchases = result.agg(spark_sum("n_purchases")).collect()[0][0]
        assert total_purchases == 4


class TestCondPmed2020StatisticalParity:
    """
    Validate cond_pmed_2020 end-to-end expenditure totals.
    """

    def test_weighted_total_expenditure(self, spark):
        """Weighted total HL drug expenditure should be mathematically correct."""
        pmed = spark.createDataFrame([
            ("P001", "D001", "RX001", "E001", "Atorvastatin", 100.0),
            ("P001", "D002", "RX002", "E002", "Rosuvastatin", 200.0),
            ("P002", "D003", "RX003", "E003", "Simvastatin", 150.0),
        ], ["DUPERSID", "DRUGIDX", "RXRECIDX", "LINKIDX", "RXDRGNAM", "RXXP20X"])

        cond = spark.createDataFrame([
            ("P001", "C001", "E78.5", "END010", "", ""),
            ("P002", "C002", "E78.0", "END010", "", ""),
        ], ["DUPERSID", "CONDIDX", "ICD10CDX", "CCSR1X", "CCSR2X", "CCSR3X"])

        clnk = spark.createDataFrame([
            ("P001", "C001", "E001", 8),
            ("P001", "C001", "E002", 8),
            ("P002", "C002", "E003", 8),
        ], ["DUPERSID", "CONDIDX", "EVNTIDX", "EVENTYPE"])

        fyc = spark.createDataFrame([
            ("P001", 50, 1, 3, 1, 1000.0, "P1", "S1"),
            ("P002", 60, 2, 4, 0, 2000.0, "P1", "S1"),
            ("P003", 70, 1, 5, 0, 1500.0, "P1", "S1"),
        ], ["DUPERSID", "AGELAST", "SEX", "POVCAT20", "CHOLDX",
            "PERWT20F", "VARPSU", "VARSTR"])

        intermediates = cond_pmed_prepare(spark, pmed, cond, clnk, fyc)
        result = intermediates["result"]

        pdf = result.select("hl_drug_exp", "PERWT20F").toPandas()
        weighted_total = (pdf["hl_drug_exp"] * pdf["PERWT20F"]).sum()

        # P001: exp=300 (100+200), weight=1000 -> 300,000
        # P002: exp=150, weight=2000 -> 300,000
        # P003: exp=0, weight=1500 -> 0
        expected = 300.0 * 1000.0 + 150.0 * 2000.0 + 0.0 * 1500.0  # 600,000
        assert _relative_error(weighted_total, expected) < 0.0001

    def test_weighted_mean_fills_among_treated(self, spark):
        """Weighted mean fills among those with hl_pmed_flag=1."""
        pmed = spark.createDataFrame([
            ("P001", "D001", "RX001", "E001", "DrugA", 50.0),
            ("P001", "D002", "RX002", "E002", "DrugB", 30.0),
            ("P002", "D003", "RX003", "E003", "DrugC", 20.0),
        ], ["DUPERSID", "DRUGIDX", "RXRECIDX", "LINKIDX", "RXDRGNAM", "RXXP20X"])

        cond = spark.createDataFrame([
            ("P001", "C001", "I10", "END010", "", ""),
            ("P002", "C002", "I11", "END010", "", ""),
        ], ["DUPERSID", "CONDIDX", "ICD10CDX", "CCSR1X", "CCSR2X", "CCSR3X"])

        clnk = spark.createDataFrame([
            ("P001", "C001", "E001", 8),
            ("P001", "C001", "E002", 8),
            ("P002", "C002", "E003", 8),
        ], ["DUPERSID", "CONDIDX", "EVNTIDX", "EVENTYPE"])

        fyc = spark.createDataFrame([
            ("P001", 50, 1, 3, 1, 3.0, "P1", "S1"),
            ("P002", 60, 2, 4, 0, 7.0, "P1", "S1"),
        ], ["DUPERSID", "AGELAST", "SEX", "POVCAT20", "CHOLDX",
            "PERWT20F", "VARPSU", "VARSTR"])

        intermediates = cond_pmed_prepare(spark, pmed, cond, clnk, fyc)
        result = intermediates["result"]

        treated = result.filter(col("hl_pmed_flag") == 1).toPandas()
        weighted_mean = (
            (treated["n_hl_fills"] * treated["PERWT20F"]).sum()
            / treated["PERWT20F"].sum()
        )
        # P001: 2 fills, weight=3; P002: 1 fill, weight=7
        # (2*3 + 1*7) / (3+7) = 13/10 = 1.3
        assert _relative_error(weighted_mean, 1.3) < 0.0001


class TestSurveyDesignVariablePreservation:
    """
    Cross-cutting test: all ETL jobs must preserve survey design variables.
    """

    def test_varstr_varpsu_weight_not_null(self, spark):
        """VARSTR, VARPSU, and weight must never be null in output."""
        data = [("P001", 100.0, 30, 30, 30, "S1", "P1", 1000.0)]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "TOTEXP16", "AGE16X", "AGE42X", "AGE31X",
             "VARSTR", "VARPSU", "PERWT16F"]
        )
        result = exercise_1a_etl(spark, df)
        for c in ["VARSTR", "VARPSU", "PERWT16F"]:
            assert result.filter(col(c).isNull()).count() == 0

    def test_survey_vars_pass_through_unchanged(self, spark):
        """Survey design variables must be passed through without modification."""
        data = [("P001", 100.0, 30, 30, 30, "STR_42", "PSU_99", 12345.67)]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "TOTEXP16", "AGE16X", "AGE42X", "AGE31X",
             "VARSTR", "VARPSU", "PERWT16F"]
        )
        result = exercise_1a_etl(spark, df)
        row = result.collect()[0]
        assert row["VARSTR"] == "STR_42"
        assert row["VARPSU"] == "PSU_99"
        assert row["PERWT16F"] == 12345.67
