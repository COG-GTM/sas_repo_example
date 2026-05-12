"""
Integration tests: Intermediate dataset parity.

For high-complexity jobs, assert row counts at each join step
match expected values from the original SAS/Stata scripts.

These tests use synthetic data with known row counts at each checkpoint,
validating the join chain logic rather than comparing to actual MEPS data.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pyspark_migration.etl.high_complexity.cond_pmed_2020 import load_and_prepare


class TestCondPmed2020Checkpoints:
    """
    Integration test: cond_pmed_2020 join chain checkpoints.

    Validates row counts at each intermediate step:
    1. After CCSR filter (hl_cond)
    2. After CLNK join (cond_clnk)
    3. After EVNTIDX de-duplication (cond_clnk_dedup)
    4. After PMED join (hl_merged / linked)
    5. After person-level collapse (person_level)
    6. After FYC left join (result)
    """

    @pytest.fixture
    def pipeline_data(self, spark):
        """
        Create a deterministic dataset where we know the exact row count
        at every checkpoint.

        Setup:
        - 5 conditions: 3 have CCSR=END010, 2 do not
        - 6 CLNK rows linking conditions to events
        - After inner join with HL conditions: 4 rows
        - After dedup on DUPERSID+EVNTIDX: 3 rows
        - 4 PMED rows; inner join with deduped: 3 matches
        - 2 unique DUPERSIDs in person_level
        - 5 FYC rows; left join preserves all 5
        """
        pmed = spark.createDataFrame([
            ("P001", "D001", "RX001", "E001", "Atorvastatin", 50.0),
            ("P001", "D002", "RX002", "E002", "Rosuvastatin", 30.0),
            ("P002", "D003", "RX003", "E003", "Simvastatin", 20.0),
            ("P003", "D004", "RX004", "E004", "Pravastatin", 15.0),
        ], ["DUPERSID", "DRUGIDX", "RXRECIDX", "LINKIDX", "RXDRGNAM", "RXXP20X"])

        cond = spark.createDataFrame([
            ("P001", "C001", "E78.5", "END010", "", ""),
            ("P001", "C002", "E78.0", "END010", "", ""),
            ("P002", "C003", "E78.1", "END010", "", ""),
            ("P003", "C004", "M79.3", "MUS001", "", ""),  # Not HL
            ("P004", "C005", "J06.9", "RES001", "", ""),  # Not HL
        ], ["DUPERSID", "CONDIDX", "ICD10CDX", "CCSR1X", "CCSR2X", "CCSR3X"])

        clnk = spark.createDataFrame([
            ("P001", "C001", "E001", 8),
            ("P001", "C002", "E001", 8),  # Duplicate EVNTIDX for P001
            ("P001", "C001", "E002", 8),
            ("P002", "C003", "E003", 8),
            ("P003", "C004", "E004", 8),  # Will be filtered (not HL)
            ("P004", "C005", "E005", 8),  # Will be filtered (not HL)
        ], ["DUPERSID", "CONDIDX", "EVNTIDX", "EVENTYPE"])

        fyc = spark.createDataFrame([
            ("P001", 50, 1, 3, 1, 1500.0, "P1", "S1"),
            ("P002", 60, 2, 4, 0, 2000.0, "P1", "S1"),
            ("P003", 70, 1, 5, 1, 1800.0, "P1", "S1"),
            ("P004", 40, 2, 2, 0, 1200.0, "P1", "S1"),
            ("P005", 30, 1, 1, 0, 900.0, "P1", "S1"),
        ], ["DUPERSID", "AGELAST", "SEX", "POVCAT20", "CHOLDX",
            "PERWT20F", "VARPSU", "VARSTR"])

        return load_and_prepare(spark, pmed, cond, clnk, fyc)

    def test_checkpoint_hl_cond_count(self, pipeline_data):
        """After CCSR filter: 3 conditions with END010."""
        assert pipeline_data["hl_cond"].count() == 3

    def test_checkpoint_cond_clnk_count(self, pipeline_data):
        """After CLNK join: 4 rows (inner join of 3 HL conds with matching CLNK)."""
        # C001->E001, C002->E001, C001->E002, C003->E003
        assert pipeline_data["cond_clnk"].count() == 4

    def test_checkpoint_dedup_count(self, pipeline_data):
        """After EVNTIDX dedup: 3 unique DUPERSID+EVNTIDX combinations."""
        # (P001,E001), (P001,E002), (P002,E003)
        assert pipeline_data["cond_clnk_dedup"].count() == 3

    def test_checkpoint_hl_merged_count(self, pipeline_data):
        """After PMED join: 3 rows (all deduped events have matching PMED)."""
        assert pipeline_data["hl_merged"].count() == 3

    def test_checkpoint_person_level_count(self, pipeline_data):
        """After person-level collapse: 2 unique DUPERSIDs (P001, P002)."""
        assert pipeline_data["person_level"].count() == 2

    def test_checkpoint_result_preserves_fyc_rows(self, pipeline_data):
        """After FYC left join: all 5 FYC rows preserved."""
        assert pipeline_data["result"].count() == 5

    def test_checkpoint_result_zero_fills(self, pipeline_data):
        """Non-matched FYC rows should have zero fills and expenses."""
        result = pipeline_data["result"]
        non_matched = result.filter(col("hl_pmed_flag") == 0)
        for row in non_matched.collect():
            assert row["n_hl_fills"] == 0
            assert row["hl_drug_exp"] == 0.0

    def test_person_level_expenditure_sums(self, pipeline_data):
        """Verify person-level expenditure sums are correct."""
        pl = pipeline_data["person_level"]
        rows = {r["DUPERSID"]: r for r in pl.collect()}
        # P001: E001(50) + E002(30) = 80
        assert rows["P001"]["hl_drug_exp"] == 80.0
        assert rows["P001"]["n_hl_fills"] == 2
        # P002: E003(20) = 20
        assert rows["P002"]["hl_drug_exp"] == 20.0
        assert rows["P002"]["n_hl_fills"] == 1


class TestExercise4aPoolingCheckpoints:
    """Integration test: Exercise 4a pooling row counts."""

    def test_pooled_union_row_count(self, spark):
        """Pooled file should contain rows from both years."""
        from pyspark_migration.etl.medium_complexity.exercise_4a import run_etl

        yr1 = spark.createDataFrame([
            ("P001", 1, 1500.0, "S1", "P1", 3, 28, 200.0),
            ("P002", 2, 2000.0, "S1", "P1", 5, 27, 100.0),
            ("P003", 3, 1800.0, "S1", "P1", 4, 40, 300.0),
        ], ["DUPERSID", "INSCOV15", "PERWT15F", "VARSTR", "VARPSU",
            "POVCAT15", "AGELAST", "TOTSLF15"])

        yr2 = spark.createDataFrame([
            ("P004", 1, 1200.0, "S1", "P1", 2, 55, 150.0),
            ("P005", 3, 900.0, "S1", "P1", 5, 29, 50.0),
        ], ["DUPERSID", "INSCOV16", "PERWT16F", "VARSTR", "VARPSU",
            "POVCAT16", "AGELAST", "TOTSLF16"])

        result = run_etl(spark, yr1, yr2)
        assert result.count() == 5  # 3 + 2

    def test_no_null_pooled_weight(self, spark):
        """POOLWT should never be null."""
        from pyspark_migration.etl.medium_complexity.exercise_4a import run_etl

        yr1 = spark.createDataFrame([
            ("P001", 1, 1500.0, "S1", "P1", 3, 28, 200.0),
        ], ["DUPERSID", "INSCOV15", "PERWT15F", "VARSTR", "VARPSU",
            "POVCAT15", "AGELAST", "TOTSLF15"])

        yr2 = spark.createDataFrame([
            ("P002", 1, 1200.0, "S1", "P1", 2, 55, 150.0),
        ], ["DUPERSID", "INSCOV16", "PERWT16F", "VARSTR", "VARPSU",
            "POVCAT16", "AGELAST", "TOTSLF16"])

        result = run_etl(spark, yr1, yr2)
        assert result.filter(col("POOLWT").isNull()).count() == 0
