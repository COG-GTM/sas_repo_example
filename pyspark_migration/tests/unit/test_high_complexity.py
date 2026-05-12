"""
Unit tests for high-complexity PySpark ETL jobs.

Tests cover:
- Schema validation
- De-duplication correctness (EVNTIDX)
- Zero-fill for non-matched FYC records
- Join chain correctness
- Row count assertions at each checkpoint
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pyspark_migration.etl.high_complexity.cond_pmed_2020 import (
    load_and_prepare as cond_pmed_prepare,
)
from pyspark_migration.etl.high_complexity.cond_mv_2020 import (
    load_and_prepare as cond_mv_prepare,
)
from pyspark_migration.etl.high_complexity.exercise_4d import (
    run_etl as exercise_4d_etl,
)


# ---------------------------------------------------------------------------
# Fixtures for common test data
# ---------------------------------------------------------------------------

@pytest.fixture
def cond_pmed_data(spark):
    """Create synthetic data for cond_pmed_2020 tests."""
    pmed = spark.createDataFrame([
        ("P001", "D001", "RX001", "E001", "Atorvastatin", 50.0),
        ("P001", "D002", "RX002", "E002", "Atorvastatin", 30.0),
        ("P002", "D003", "RX003", "E003", "Lisinopril", 20.0),
    ], ["DUPERSID", "DRUGIDX", "RXRECIDX", "LINKIDX", "RXDRGNAM", "RXXP20X"])

    cond = spark.createDataFrame([
        ("P001", "C001", "I10", "END010", "", ""),
        ("P001", "C002", "I11", "END010", "", ""),
        ("P002", "C003", "I12", "MUS001", "", ""),
        ("P003", "C004", "I13", "END010", "", ""),
    ], ["DUPERSID", "CONDIDX", "ICD10CDX", "CCSR1X", "CCSR2X", "CCSR3X"])

    clnk = spark.createDataFrame([
        ("P001", "C001", "E001", 8),
        ("P001", "C002", "E001", 8),  # Same EVNTIDX -> will be deduped
        ("P001", "C001", "E002", 8),
        ("P003", "C004", "E999", 8),  # No matching PMED
    ], ["DUPERSID", "CONDIDX", "EVNTIDX", "EVENTYPE"])

    fyc = spark.createDataFrame([
        ("P001", 50, 1, 3, 1, 1500.0, "P1", "S1"),
        ("P002", 60, 2, 4, 0, 2000.0, "P1", "S1"),
        ("P003", 70, 1, 5, 1, 1800.0, "P1", "S1"),
        ("P004", 40, 2, 2, 0, 1200.0, "P1", "S1"),  # No conditions at all
    ], ["DUPERSID", "AGELAST", "SEX", "POVCAT20", "CHOLDX",
        "PERWT20F", "VARPSU", "VARSTR"])

    return pmed, cond, clnk, fyc


# ---------------------------------------------------------------------------
# cond_pmed_2020 tests
# ---------------------------------------------------------------------------

class TestCondPmed2020:
    """Tests for cond_pmed_2020: Prescribed medicines for hyperlipidemia."""

    def test_ccsr_filter(self, spark, cond_pmed_data):
        """Only conditions with CCSR = END010 should pass the filter."""
        pmed, cond, clnk, fyc = cond_pmed_data
        intermediates = cond_pmed_prepare(spark, pmed, cond, clnk, fyc)
        hl_cond = intermediates["hl_cond"]
        # P001 has C001 and C002 with END010, P003 has C004 with END010
        assert hl_cond.count() == 3
        # P002's condition (MUS001) should be filtered out
        assert hl_cond.filter(col("DUPERSID") == "P002").count() == 0

    def test_dedup_on_evntidx(self, spark, cond_pmed_data):
        """After de-duplication on DUPERSID+EVNTIDX, no duplicates should remain."""
        pmed, cond, clnk, fyc = cond_pmed_data
        intermediates = cond_pmed_prepare(spark, pmed, cond, clnk, fyc)
        deduped = intermediates["cond_clnk_dedup"]

        # Check no duplicate DUPERSID+EVNTIDX
        total = deduped.count()
        distinct = deduped.select("DUPERSID", "EVNTIDX").distinct().count()
        assert total == distinct

    def test_zero_fill_for_nonmatched_fyc(self, spark, cond_pmed_data):
        """Persons in FYC with no PMED fills must get n_hl_fills=0, hl_drug_exp=0."""
        pmed, cond, clnk, fyc = cond_pmed_data
        intermediates = cond_pmed_prepare(spark, pmed, cond, clnk, fyc)
        result = intermediates["result"]

        # P004 has no conditions -> should have zeros
        p4 = result.filter(col("DUPERSID") == "P004").collect()[0]
        assert p4["n_hl_fills"] == 0
        assert p4["hl_drug_exp"] == 0.0
        assert p4["hl_pmed_flag"] == 0

    def test_fyc_row_count_preserved(self, spark, cond_pmed_data):
        """Left join to FYC must preserve all FYC rows."""
        pmed, cond, clnk, fyc = cond_pmed_data
        intermediates = cond_pmed_prepare(spark, pmed, cond, clnk, fyc)
        result = intermediates["result"]
        assert result.count() == fyc.count()

    def test_person_level_collapse(self, spark, cond_pmed_data):
        """Person-level collapse should produce one row per DUPERSID."""
        pmed, cond, clnk, fyc = cond_pmed_data
        intermediates = cond_pmed_prepare(spark, pmed, cond, clnk, fyc)
        pl = intermediates["person_level"]
        total = pl.count()
        distinct = pl.select("DUPERSID").distinct().count()
        assert total == distinct

    def test_hl_pmed_flag(self, spark, cond_pmed_data):
        """hl_pmed_flag should be 1 for persons with fills, 0 otherwise."""
        pmed, cond, clnk, fyc = cond_pmed_data
        intermediates = cond_pmed_prepare(spark, pmed, cond, clnk, fyc)
        result = intermediates["result"]

        # P001 has fills -> flag=1
        p1 = result.filter(col("DUPERSID") == "P001").collect()[0]
        assert p1["hl_pmed_flag"] == 1

        # P002 has no HL conditions -> flag=0
        p2 = result.filter(col("DUPERSID") == "P002").collect()[0]
        assert p2["hl_pmed_flag"] == 0

    def test_no_null_dupersid(self, spark, cond_pmed_data):
        """DUPERSID should never be null in any intermediate."""
        pmed, cond, clnk, fyc = cond_pmed_data
        intermediates = cond_pmed_prepare(spark, pmed, cond, clnk, fyc)
        for name, df in intermediates.items():
            id_col = "DUPERSID" if "DUPERSID" in df.columns else "dupersid"
            if id_col in df.columns:
                null_count = df.filter(col(id_col).isNull()).count()
                assert null_count == 0, f"Null DUPERSID in {name}"

    def test_no_null_survey_vars_in_result(self, spark, cond_pmed_data):
        """Survey design vars must not be null in final result."""
        pmed, cond, clnk, fyc = cond_pmed_data
        intermediates = cond_pmed_prepare(spark, pmed, cond, clnk, fyc)
        result = intermediates["result"]
        null_count = result.filter(
            col("VARSTR").isNull() | col("VARPSU").isNull() | col("PERWT20F").isNull()
        ).count()
        assert null_count == 0


# ---------------------------------------------------------------------------
# cond_mv_2020 tests (Stata equivalent)
# ---------------------------------------------------------------------------

class TestCondMv2020:
    """Tests for cond_mv_2020.do: Office-based visits for mental health (Stata)."""

    def test_dedup_on_evntidx_stata(self, spark):
        """Stata: duplicates drop evntidx, force -> one row per evntidx."""
        pmed = spark.createDataFrame([
            ("P001", "D001", "RX001", "E001", "DrugA", 50.0),
        ], ["DUPERSID", "DRUGIDX", "RXRECIDX", "LINKIDX", "RXDRGNAM", "RXXP20X"])

        cond = spark.createDataFrame([
            ("P001", "C001", "I10", "END010", "", ""),
            ("P001", "C002", "I11", "END010", "", ""),
        ], ["DUPERSID", "CONDIDX", "ICD10CDX", "CCSR1X", "CCSR2X", "CCSR3X"])

        clnk = spark.createDataFrame([
            ("P001", "C001", "E001", 8),
            ("P001", "C002", "E001", 8),  # duplicate evntidx
        ], ["DUPERSID", "CONDIDX", "EVNTIDX", "EVENTYPE"])

        fyc = spark.createDataFrame([
            ("P001", 1, 50, 1, 3, "S1", "P1", 1500.0),
        ], ["DUPERSID", "SEX", "AGELAST", "CHOLDX", "POVCAT20",
            "VARSTR", "VARPSU", "PERWT20F"])

        intermediates = cond_mv_prepare(spark, pmed, cond, clnk, fyc)
        deduped = intermediates["cond_clnk_dedup"]
        # Only one E001 after dedup
        assert deduped.count() == 1

    def test_zero_fill_stata(self, spark):
        """Stata: replace exp_rx=0 if _merge==2."""
        pmed = spark.createDataFrame(
            [], "DUPERSID STRING, DRUGIDX STRING, RXRECIDX STRING, "
                "LINKIDX STRING, RXDRGNAM STRING, RXXP20X DOUBLE"
        )
        cond = spark.createDataFrame(
            [], "DUPERSID STRING, CONDIDX STRING, ICD10CDX STRING, "
                "CCSR1X STRING, CCSR2X STRING, CCSR3X STRING"
        )
        clnk = spark.createDataFrame(
            [], "DUPERSID STRING, CONDIDX STRING, EVNTIDX STRING, EVENTYPE INT"
        )
        fyc = spark.createDataFrame([
            ("P001", 1, 50, 1, 3, "S1", "P1", 1500.0),
        ], ["DUPERSID", "SEX", "AGELAST", "CHOLDX", "POVCAT20",
            "VARSTR", "VARPSU", "PERWT20F"])

        intermediates = cond_mv_prepare(spark, pmed, cond, clnk, fyc)
        result = intermediates["result"]
        row = result.collect()[0]
        assert row["num_rx"] == 0
        assert row["exp_rx"] == 0.0
        assert row["any_rx"] == 0


# ---------------------------------------------------------------------------
# Exercise 4d: Pooling 2017-2019 tests
# ---------------------------------------------------------------------------

class TestExercise4d:
    """Tests for Exercise4d: Pooling FYC files 2017-2019."""

    def _make_fyc(self, spark, year):
        suffix = str(year)[-2:]
        if year == 2017:
            data = [("P001", 21, "S1", "P1", 1500.0, 50, 1, 1, 5000.0, 200.0)]
            cols = ["DUPERSID", "PANEL", "VARSTR", "VARPSU", f"PERWT{suffix}F",
                    "AGELAST", "ARTHDX", "JTPAIN31", f"TOTEXP{suffix}", f"TOTSLF{suffix}"]
        else:
            data = [("P002", 22, "S1", "P1", 2000.0, 60, 2, -1, 3000.0, 150.0)]
            cols = ["DUPERSID", "PANEL", "VARSTR", "VARPSU", f"PERWT{suffix}F",
                    "AGELAST", "ARTHDX", "JTPAIN31_M18", f"TOTEXP{suffix}", f"TOTSLF{suffix}"]
        return spark.createDataFrame(data, cols)

    def test_pooled_weight_divided_by_three(self, spark):
        yr17 = self._make_fyc(spark, 2017)
        yr18 = self._make_fyc(spark, 2018)
        yr19 = self._make_fyc(spark, 2019)
        result = exercise_4d_etl(spark, yr17, yr18, yr19)
        # P001 weight 1500/3 = 500
        row = result.filter(col("year") == 2017).collect()[0]
        assert row["perwtf"] == 500.0

    def test_three_years_pooled(self, spark):
        yr17 = self._make_fyc(spark, 2017)
        yr18 = self._make_fyc(spark, 2018)
        yr19 = self._make_fyc(spark, 2019)
        result = exercise_4d_etl(spark, yr17, yr18, yr19)
        years = [r["year"] for r in result.select("year").distinct().collect()]
        assert set(years) == {2017, 2018, 2019}

    def test_spop_adult(self, spark):
        """SPOP=1 for adults age >= 18 with valid ARTHDX/JTPAIN responses."""
        data = [("P001", 22, "S1", "P1", 1500.0, 50, 1, 1, 5000.0, 200.0)]
        cols = ["DUPERSID", "PANEL", "VARSTR", "VARPSU", "PERWT18F",
                "AGELAST", "ARTHDX", "JTPAIN31_M18", "TOTEXP18", "TOTSLF18"]
        yr18 = spark.createDataFrame(data, cols)

        # Need at least 3 year DFs
        empty17 = spark.createDataFrame(
            [], "DUPERSID STRING, PANEL INT, VARSTR STRING, VARPSU STRING, "
                "PERWT17F DOUBLE, AGELAST INT, ARTHDX INT, JTPAIN31 INT, "
                "TOTEXP17 DOUBLE, TOTSLF17 DOUBLE"
        )
        empty19 = spark.createDataFrame(
            [], "DUPERSID STRING, PANEL INT, VARSTR STRING, VARPSU STRING, "
                "PERWT19F DOUBLE, AGELAST INT, ARTHDX INT, JTPAIN31_M18 INT, "
                "TOTEXP19 DOUBLE, TOTSLF19 DOUBLE"
        )

        result = exercise_4d_etl(spark, empty17, yr18, empty19)
        row = result.collect()[0]
        assert row["spop"] == 1
        assert row["joint_pain"] == 1  # ARTHDX=1
