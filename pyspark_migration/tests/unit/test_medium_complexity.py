"""
Unit tests for medium-complexity PySpark ETL jobs.

Tests cover:
- Schema validation
- Row count assertions
- Null/zero checks
- Aggregation correctness
- Pooling logic
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pyspark_migration.etl.medium_complexity.pmed_prescribed_drug_2016 import (
    run_etl as pmed_drug_etl,
)
from pyspark_migration.etl.medium_complexity.exercise_4a import (
    run_etl as exercise_4a_etl,
)
from pyspark_migration.etl.medium_complexity.exercise_5a import (
    run_etl as exercise_5a_etl,
)
from pyspark_migration.etl.medium_complexity.exercise_5b import (
    run_etl as exercise_5b_etl,
)
from pyspark_migration.etl.medium_complexity.exercise_4b import (
    run_etl as exercise_4b_etl,
)


# ---------------------------------------------------------------------------
# PMED Prescribed Drug 2016 tests
# ---------------------------------------------------------------------------

class TestPmedDrug2016:
    """Tests for pmed_prescribed_drug_2016: Drug purchases/expenditures."""

    def test_person_drug_aggregation(self, spark):
        """Multiple fills per person per drug should be summed."""
        data = [
            ("P001", "S1", "P1", 1000.0, "Atorvastatin", 50.0),
            ("P001", "S1", "P1", 1000.0, "Atorvastatin", 30.0),
            ("P001", "S1", "P1", 1000.0, "Lisinopril", 20.0),
            ("P002", "S1", "P1", 2000.0, "Atorvastatin", 45.0),
        ]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "VARSTR", "VARPSU", "PERWT16F", "RXDRGNAM", "RXXP16X"]
        )
        result = pmed_drug_etl(spark, df)

        # P001 + Atorvastatin: sum=80, count=2
        p1_ator = result.filter(
            (col("DUPERSID") == "P001") & (col("RXDRGNAM") == "Atorvastatin")
        ).collect()[0]
        assert p1_ator["pers_RXXP"] == 80.0
        assert p1_ator["n_purchases"] == 2

        # P001 + Lisinopril: sum=20, count=1
        p1_lis = result.filter(
            (col("DUPERSID") == "P001") & (col("RXDRGNAM") == "Lisinopril")
        ).collect()[0]
        assert p1_lis["pers_RXXP"] == 20.0
        assert p1_lis["n_purchases"] == 1

    def test_person_flag_added(self, spark):
        data = [("P001", "S1", "P1", 1000.0, "DrugA", 50.0)]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "VARSTR", "VARPSU", "PERWT16F", "RXDRGNAM", "RXXP16X"]
        )
        result = pmed_drug_etl(spark, df)
        assert result.collect()[0]["person"] == 1

    def test_output_row_count_per_person_drug(self, spark):
        data = [
            ("P001", "S1", "P1", 1000.0, "DrugA", 50.0),
            ("P001", "S1", "P1", 1000.0, "DrugA", 30.0),
            ("P001", "S1", "P1", 1000.0, "DrugB", 20.0),
        ]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "VARSTR", "VARPSU", "PERWT16F", "RXDRGNAM", "RXXP16X"]
        )
        result = pmed_drug_etl(spark, df)
        assert result.count() == 2  # Two unique person-drug combinations


# ---------------------------------------------------------------------------
# Exercise 4a: Pooling 2015-2016 tests
# ---------------------------------------------------------------------------

class TestExercise4a:
    """Tests for Exercise4a: Pooling FYC files 2015+2016."""

    def _make_yr_df(self, spark, year):
        suffix = str(year)[-2:]
        data = [
            ("P001", 1, 1500.0, "S1", "P1", 3, 28, 200.0),
            ("P002", 2, 2000.0, "S1", "P1", 5, 27, 100.0),
        ]
        cols = [
            "DUPERSID", f"INSCOV{suffix}", f"PERWT{suffix}F",
            "VARSTR", "VARPSU", f"POVCAT{suffix}", "AGELAST", f"TOTSLF{suffix}"
        ]
        return spark.createDataFrame(data, cols)

    def test_pooled_row_count(self, spark):
        yr1 = self._make_yr_df(spark, 2015)
        yr2 = self._make_yr_df(spark, 2016)
        result = exercise_4a_etl(spark, yr1, yr2)
        assert result.count() == 4  # 2 + 2

    def test_pooled_weight_halved(self, spark):
        yr1 = self._make_yr_df(spark, 2015)
        yr2 = self._make_yr_df(spark, 2016)
        result = exercise_4a_etl(spark, yr1, yr2)
        row = result.filter(col("DUPERSID") == "P001").collect()[0]
        assert row["POOLWT"] == 750.0  # 1500 / 2

    def test_columns_renamed(self, spark):
        yr1 = self._make_yr_df(spark, 2015)
        yr2 = self._make_yr_df(spark, 2016)
        result = exercise_4a_etl(spark, yr1, yr2)
        assert "INSCOV" in result.columns
        assert "PERWT" in result.columns
        assert "POVCAT" in result.columns
        assert "TOTSLF" in result.columns

    def test_subpop_flag(self, spark):
        """SUBPOP=1 when age 26-30, POVCAT=5, INSCOV=3."""
        data_yr1 = [
            ("P001", 3, 1500.0, "S1", "P1", 5, 28, 200.0),
            ("P002", 1, 2000.0, "S1", "P1", 3, 35, 100.0),
        ]
        yr1 = spark.createDataFrame(
            data_yr1,
            ["DUPERSID", "INSCOV15", "PERWT15F", "VARSTR", "VARPSU",
             "POVCAT15", "AGELAST", "TOTSLF15"]
        )
        yr2 = spark.createDataFrame([], yr1.schema.add("dummy", "string"))
        # Use yr1 for both to simplify; yr2 empty
        data_yr2 = [("P003", 2, 1000.0, "S1", "P1", 4, 29, 50.0)]
        yr2 = spark.createDataFrame(
            data_yr2,
            ["DUPERSID", "INSCOV16", "PERWT16F", "VARSTR", "VARPSU",
             "POVCAT16", "AGELAST", "TOTSLF16"]
        )
        result = exercise_4a_etl(spark, yr1, yr2)
        rows = {r["DUPERSID"]: r for r in result.collect()}
        assert rows["P001"]["SUBPOP"] == 1  # age=28, POVCAT=5, INSCOV=3
        assert rows["P002"]["SUBPOP"] == 2  # age=35, wrong age


# ---------------------------------------------------------------------------
# Exercise 5a: Family-level variables tests
# ---------------------------------------------------------------------------

class TestExercise5a:
    """Tests for Exercise5a: Construct family-level variables."""

    def test_family_aggregation(self, spark):
        data = [
            ("P001", "D001", "F01", 1500.0, "S1", "P1", 200.0, 30000.0),
            ("P002", "D001", "F01", 1500.0, "S1", "P1", 100.0, 25000.0),
            ("P003", "D002", "F01", 2000.0, "S1", "P1", 50.0, 40000.0),
        ]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "DUID", "CPSFAMID", "FAMWT15C",
             "VARSTR", "VARPSU", "TOTSLF15", "TTLP15X"]
        )
        result = exercise_5a_etl(spark, df)

        # Family D001-F01: 2 persons, FAMOOP=300, FAMINC=55000
        fam1 = result.filter(
            (col("DUID") == "D001") & (col("CPSFAMID") == "F01")
        ).collect()[0]
        assert fam1["FAMSIZE"] == 2
        assert fam1["FAMOOP"] == 300.0
        assert fam1["FAMINC"] == 55000.0


# ---------------------------------------------------------------------------
# Exercise 5b: Insurance status from monthly variables tests
# ---------------------------------------------------------------------------

class TestExercise5b:
    """Tests for Exercise5b: Insurance status from monthly variables."""

    def test_insurance_flags(self, spark):
        """Test insurance flag creation with synthetic data."""
        # Create data with INS month columns (all insured = 1)
        months = ["JA", "FE", "MA", "AP", "MY", "JU", "JL", "AU", "SE", "OC", "NO", "DE"]
        cols = ["DUPERSID", "RACETHX", "VARSTR", "VARPSU", "PERWT15F"]
        for m in months:
            cols.append(f"INS{m}15X")
        for m in months:
            cols.append(f"PRI{m}15")

        data = [("P001", 1, "S1", "P1", 1000.0) + (1,) * 12 + (1,) * 12]
        df = spark.createDataFrame(data, cols)

        # Add remaining columns as zeros
        for prefix in ["PEG", "TRI", "POU", "PDK", "PRX", "PNG", "POG", "PRS",
                        "MCR", "MCD", "OPA", "OPB"]:
            for m in months:
                suffix = "15X" if prefix in ["MCR", "MCD", "TRI"] else "15"
                col_name = f"{prefix}{m}{suffix}"
                if col_name not in df.columns:
                    from pyspark.sql.functions import lit
                    df = df.withColumn(col_name, lit(0))

        result = exercise_5b_etl(spark, df)
        row = result.collect()[0]
        # All 12 months insured -> FULL_INSU=1
        assert row["FULL_INSU"] == 1
        assert row["UNINS_N"] == 0
        assert row["INS_N"] == 12


# ---------------------------------------------------------------------------
# Exercise 4b: COVID care delay regression tests
# ---------------------------------------------------------------------------

class TestExercise4b:
    """Tests for exercise_4b.R: COVID care delay analysis, 2020."""

    def test_outcome_conversion(self, spark):
        """CVDLAY values should be converted from 1/2 to 1/0."""
        data = [
            ("P001", "P1", "S1", 1000.0, 1, 2, 1, 30, 1, 2, 1, 3),
            ("P002", "P1", "S1", 2000.0, 2, 1, 2, 45, 2, 1, 2, 1),
        ]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "VARPSU", "VARSTR", "PERWT20F",
             "CVDLAYCA53", "CVDLAYDN53", "CVDLAYPM53",
             "AGELAST", "SEX", "RACETHX", "INSCOV20", "REGION53"]
        )
        result = exercise_4b_etl(spark, df)
        rows = {r["DUPERSID"]: r for r in result.collect()}

        assert rows["P001"]["covid_delay_CARE"] == 1
        assert rows["P001"]["covid_delay_DENTAL"] == 0
        assert rows["P002"]["covid_delay_CARE"] == 0
        assert rows["P002"]["covid_delay_DENTAL"] == 1

    def test_subpop_flags(self, spark):
        """Subpop should be 1 when CVDLAY >= 0, 0 otherwise."""
        data = [
            ("P001", "P1", "S1", 1000.0, 1, -1, 2, 30, 1, 2, 1, 3),
        ]
        df = spark.createDataFrame(
            data,
            ["DUPERSID", "VARPSU", "VARSTR", "PERWT20F",
             "CVDLAYCA53", "CVDLAYDN53", "CVDLAYPM53",
             "AGELAST", "SEX", "RACETHX", "INSCOV20", "REGION53"]
        )
        result = exercise_4b_etl(spark, df)
        row = result.collect()[0]
        assert row["subpop_CARE"] == 1
        assert row["subpop_DENTAL"] == 0  # -1 < 0
        assert row["subpop_PMED"] == 1
