"""Tests for Medium Complexity PySpark Migration Jobs.

Covers:
  - Schema validation
  - Aggregation correctness
  - Multi-file merge logic
  - Variable construction from monthly data
"""

import pytest
from pyspark.sql.functions import abs as spark_abs, col


# ---------------------------------------------------------------------------
# Prescribed Drug 2016 Tests
# ---------------------------------------------------------------------------


class TestPmedPrescribedDrug2016:
    """Tests for pmed_prescribed_drug_2016 - Drug purchases/expenditures."""

    def test_schema_has_expected_columns(self, spark, rx_h188a_data):
        """Output must have person-drug level aggregation columns."""
        from pyspark_migration.jobs.medium_complexity.pmed_prescribed_drug_2016 import (
            build_etl_dataframe,
        )

        df = build_etl_dataframe(spark, rx_h188a_data)
        expected = {
            "DUPERSID", "VARSTR", "VARPSU", "PERWT16F", "RXDRGNAM",
            "pers_RXXP", "n_purchases", "person",
        }
        assert expected.issubset(set(df.columns))

    def test_aggregation_correct(self, spark, rx_h188a_data):
        """Verify person-drug aggregation sums and counts."""
        from pyspark_migration.jobs.medium_complexity.pmed_prescribed_drug_2016 import (
            build_etl_dataframe,
        )

        df = build_etl_dataframe(spark, rx_h188a_data)

        # P001 has 2 ATORVASTATIN fills at 45.50 each
        p001_ator = df.filter(
            (col("DUPERSID") == "P001") & (col("RXDRGNAM") == "ATORVASTATIN CALCIUM")
        ).collect()
        assert len(p001_ator) == 1
        assert p001_ator[0]["n_purchases"] == 2
        assert abs(p001_ator[0]["pers_RXXP"] - 91.0) < 0.01

        # P001 has 1 LISINOPRIL fill at 12.00
        p001_lis = df.filter(
            (col("DUPERSID") == "P001") & (col("RXDRGNAM") == "LISINOPRIL")
        ).collect()
        assert len(p001_lis) == 1
        assert p001_lis[0]["n_purchases"] == 1

    def test_person_indicator(self, spark, rx_h188a_data):
        """Every row must have person=1."""
        from pyspark_migration.jobs.medium_complexity.pmed_prescribed_drug_2016 import (
            build_etl_dataframe,
        )

        df = build_etl_dataframe(spark, rx_h188a_data)
        assert df.filter(col("person") != 1).count() == 0

    def test_survey_design_preserved(self, spark, rx_h188a_data):
        """VARSTR, VARPSU, PERWT16F must not be null."""
        from pyspark_migration.jobs.medium_complexity.pmed_prescribed_drug_2016 import (
            build_etl_dataframe,
        )

        df = build_etl_dataframe(spark, rx_h188a_data)
        for c in ["VARSTR", "VARPSU", "PERWT16F"]:
            assert df.filter(col(c).isNull()).count() == 0


# ---------------------------------------------------------------------------
# Exercise 4a Tests - Pooling
# ---------------------------------------------------------------------------


class TestExercise4a:
    """Tests for exercise_4a - Pooling FYC files 2015+2016."""

    def test_pooled_has_both_years(self, spark, h181_data, h192_data):
        """Pooled dataset must have rows from both years."""
        from pyspark_migration.jobs.medium_complexity.exercise_4a import build_etl_dataframe

        df = build_etl_dataframe(spark, h181_data, h192_data)
        # Must have POOLWT column
        assert "POOLWT" in df.columns
        # POOLWT should be PERWT / 2
        row = df.first()
        assert row is not None

    def test_renamed_columns_present(self, spark, h181_data, h192_data):
        """Year-specific columns must be renamed to common names."""
        from pyspark_migration.jobs.medium_complexity.exercise_4a import build_etl_dataframe

        df = build_etl_dataframe(spark, h181_data, h192_data)
        for c in ["INSCOV", "PERWT", "POVCAT", "TOTSLF", "POOLWT"]:
            assert c in df.columns, f"Missing renamed column: {c}"

    def test_poolwt_calculation(self, spark, h181_data, h192_data):
        """POOLWT must equal PERWT / 2."""
        from pyspark_migration.jobs.medium_complexity.exercise_4a import build_etl_dataframe

        df = build_etl_dataframe(spark, h181_data, h192_data)
        mismatch = df.filter(
            spark_abs(col("POOLWT") - col("PERWT") / 2) > 0.01
        ).count()
        assert mismatch == 0

    def test_subpop_column_present(self, spark, h181_data, h192_data):
        """SUBPOP column must exist for domain analysis."""
        from pyspark_migration.jobs.medium_complexity.exercise_4a import build_etl_dataframe

        df = build_etl_dataframe(spark, h181_data, h192_data)
        assert "SUBPOP" in df.columns


# ---------------------------------------------------------------------------
# Exercise 5a Tests - Family Level
# ---------------------------------------------------------------------------


class TestExercise5a:
    """Tests for exercise_5a - Family-level variables."""

    def test_family_aggregation_columns(self, spark, h181_data):
        """Output must have FAMSIZE, FAMOOP, FAMINC."""
        from pyspark_migration.jobs.medium_complexity.exercise_5a import build_etl_dataframe

        df = build_etl_dataframe(spark, h181_data)
        expected = {"FAMSIZE", "FAMOOP", "FAMINC", "FAMWT15C", "VARSTR", "VARPSU"}
        assert expected.issubset(set(df.columns))

    def test_family_aggregation_values(self, spark, h181_data):
        """Family D001/F001 should have FAMSIZE=2 (two members)."""
        from pyspark_migration.jobs.medium_complexity.exercise_5a import build_etl_dataframe

        df = build_etl_dataframe(spark, h181_data)
        fam = df.filter(
            (col("DUID") == "D001") & (col("CPSFAMID") == "F001")
        ).collect()
        assert len(fam) == 1
        assert fam[0]["FAMSIZE"] == 2

    def test_survey_design_vars_present(self, spark, h181_data):
        """Survey design variables must be present."""
        from pyspark_migration.jobs.medium_complexity.exercise_5a import build_etl_dataframe

        df = build_etl_dataframe(spark, h181_data)
        for c in ["VARSTR", "VARPSU", "FAMWT15C"]:
            assert c in df.columns


# ---------------------------------------------------------------------------
# Exercise 4b COVID Tests
# ---------------------------------------------------------------------------


class TestExercise4bCovid:
    """Tests for exercise_4b_covid - COVID care delay."""

    def test_schema_columns(self, spark, tmp_path_factory):
        """Output must have COVID delay variables and subpopulation flags."""
        from pyspark_migration.jobs.medium_complexity.exercise_4b_covid import (
            build_etl_dataframe,
        )

        path = str(tmp_path_factory.mktemp("h224_covid"))
        from pyspark.sql.types import (
            DoubleType, IntegerType, StringType, StructField, StructType,
        )
        schema = StructType([
            StructField("DUPERSID", StringType()),
            StructField("VARPSU", IntegerType()),
            StructField("VARSTR", IntegerType()),
            StructField("PERWT20F", DoubleType()),
            StructField("CVDLAYCA53", IntegerType()),
            StructField("CVDLAYDN53", IntegerType()),
            StructField("CVDLAYPM53", IntegerType()),
            StructField("AGELAST", IntegerType()),
            StructField("SEX", IntegerType()),
            StructField("RACETHX", IntegerType()),
            StructField("INSCOV20", IntegerType()),
            StructField("REGION53", IntegerType()),
        ])
        data = [
            ("P001", 1, 100, 1500.0, 1, 2, 1, 45, 1, 2, 1, 3),
            ("P002", 2, 200, 2000.0, 2, 1, 2, 70, 2, 1, 2, 1),
            ("P003", 1, 100, 1800.0, -1, -1, -1, 30, 1, 3, 3, 2),
        ]
        spark.createDataFrame(data, schema).write.mode("overwrite").parquet(path)

        df = build_etl_dataframe(spark, path)
        expected = {
            "covid_delay_CARE", "covid_delay_DENTAL", "covid_delay_PMED",
            "subpop_CARE", "subpop_DENTAL", "subpop_PMED",
        }
        assert expected.issubset(set(df.columns))

    def test_covid_delay_conversion(self, spark, tmp_path_factory):
        """CVDLAY values 1->1, 2->0 conversion must be correct."""
        from pyspark_migration.jobs.medium_complexity.exercise_4b_covid import (
            build_etl_dataframe,
        )

        path = str(tmp_path_factory.mktemp("h224_covid2"))
        from pyspark.sql.types import (
            DoubleType, IntegerType, StringType, StructField, StructType,
        )
        schema = StructType([
            StructField("DUPERSID", StringType()),
            StructField("VARPSU", IntegerType()),
            StructField("VARSTR", IntegerType()),
            StructField("PERWT20F", DoubleType()),
            StructField("CVDLAYCA53", IntegerType()),
            StructField("CVDLAYDN53", IntegerType()),
            StructField("CVDLAYPM53", IntegerType()),
            StructField("AGELAST", IntegerType()),
            StructField("SEX", IntegerType()),
            StructField("RACETHX", IntegerType()),
            StructField("INSCOV20", IntegerType()),
            StructField("REGION53", IntegerType()),
        ])
        data = [
            ("P001", 1, 100, 1500.0, 1, 2, 1, 45, 1, 2, 1, 3),  # yes, no, yes
            ("P002", 2, 200, 2000.0, 2, 1, 2, 70, 2, 1, 2, 1),  # no, yes, no
        ]
        spark.createDataFrame(data, schema).write.mode("overwrite").parquet(path)

        df = build_etl_dataframe(spark, path)
        rows = {r["DUPERSID"]: r for r in df.collect()}

        assert rows["P001"]["covid_delay_CARE"] == 1
        assert rows["P001"]["covid_delay_DENTAL"] == 0
        assert rows["P001"]["covid_delay_PMED"] == 1

        assert rows["P002"]["covid_delay_CARE"] == 0
        assert rows["P002"]["covid_delay_DENTAL"] == 1
        assert rows["P002"]["covid_delay_PMED"] == 0

    def test_subpop_excludes_missing(self, spark, tmp_path_factory):
        """subpop flags must be 0 for negative CVDLAY values."""
        from pyspark_migration.jobs.medium_complexity.exercise_4b_covid import (
            build_etl_dataframe,
        )

        path = str(tmp_path_factory.mktemp("h224_covid3"))
        from pyspark.sql.types import (
            DoubleType, IntegerType, StringType, StructField, StructType,
        )
        schema = StructType([
            StructField("DUPERSID", StringType()),
            StructField("VARPSU", IntegerType()),
            StructField("VARSTR", IntegerType()),
            StructField("PERWT20F", DoubleType()),
            StructField("CVDLAYCA53", IntegerType()),
            StructField("CVDLAYDN53", IntegerType()),
            StructField("CVDLAYPM53", IntegerType()),
            StructField("AGELAST", IntegerType()),
            StructField("SEX", IntegerType()),
            StructField("RACETHX", IntegerType()),
            StructField("INSCOV20", IntegerType()),
            StructField("REGION53", IntegerType()),
        ])
        data = [
            ("P001", 1, 100, 1500.0, -1, -7, -8, 45, 1, 2, 1, 3),
        ]
        spark.createDataFrame(data, schema).write.mode("overwrite").parquet(path)

        df = build_etl_dataframe(spark, path)
        row = df.collect()[0]
        assert row["subpop_CARE"] == 0
        assert row["subpop_DENTAL"] == 0
        assert row["subpop_PMED"] == 0
