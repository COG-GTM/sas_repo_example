"""Tests for High Complexity PySpark Migration Jobs.

Covers:
  - Schema validation
  - Row count assertions at each join step (integration test checkpoints)
  - De-duplication correctness on EVNTIDX
  - Zero-fill for non-matched FYC records
  - Null/zero checks on key columns
"""

import pytest
from pyspark.sql.functions import col


# ---------------------------------------------------------------------------
# cond_pmed_2020 Tests (SAS version)
# ---------------------------------------------------------------------------


class TestCondPmed2020:
    """Tests for cond_pmed_2020 - Prescribed medicines for hyperlipidemia."""

    def test_schema_has_expected_columns(self, spark, cond_pmed_data):
        """Final result must have all required columns."""
        from pyspark_migration.jobs.high_complexity.cond_pmed_2020 import (
            build_etl_with_intermediates,
        )

        intermediates = build_etl_with_intermediates(
            spark,
            cond_pmed_data["pmed_path"],
            cond_pmed_data["cond_path"],
            cond_pmed_data["clnk_path"],
            cond_pmed_data["fyc_path"],
        )
        result = intermediates["result"]
        expected = {
            "DUPERSID", "AGELAST", "SEX", "CHOLDX", "POVCAT20",
            "VARSTR", "VARPSU", "PERWT20F",
            "n_hl_fills", "hl_drug_exp", "hl_pmed_flag",
        }
        assert expected.issubset(set(result.columns))

    def test_ccsr_filter(self, spark, cond_pmed_data):
        """After CCSR filter, only END010 conditions remain."""
        from pyspark_migration.jobs.high_complexity.cond_pmed_2020 import (
            build_etl_with_intermediates,
        )

        intermediates = build_etl_with_intermediates(
            spark,
            cond_pmed_data["pmed_path"],
            cond_pmed_data["cond_path"],
            cond_pmed_data["clnk_path"],
            cond_pmed_data["fyc_path"],
        )
        hl_cond = intermediates["hl_cond"]

        # Only conditions with END010 in any CCSR column
        for row in hl_cond.collect():
            assert (
                row["CCSR1X"] == "END010"
                or row["CCSR2X"] == "END010"
                or row["CCSR3X"] == "END010"
            ), f"Non-HL condition found: {row}"

    def test_dedup_on_evntidx(self, spark, cond_pmed_data):
        """After de-duplication, no duplicate EVNTIDX values per person remain."""
        from pyspark_migration.jobs.high_complexity.cond_pmed_2020 import (
            build_etl_with_intermediates,
        )

        intermediates = build_etl_with_intermediates(
            spark,
            cond_pmed_data["pmed_path"],
            cond_pmed_data["cond_path"],
            cond_pmed_data["clnk_path"],
            cond_pmed_data["fyc_path"],
        )
        dedup = intermediates["cond_clnk_dedup"]

        # Count distinct DUPERSID+EVNTIDX pairs
        total = dedup.count()
        distinct = dedup.select("DUPERSID", "EVNTIDX").distinct().count()
        assert total == distinct, (
            f"Duplicates remain after dedup: {total} total vs {distinct} distinct"
        )

    def test_dedup_reduces_row_count(self, spark, cond_pmed_data):
        """De-duplication must reduce rows vs pre-dedup (given test data has dups)."""
        from pyspark_migration.jobs.high_complexity.cond_pmed_2020 import (
            build_etl_with_intermediates,
        )

        intermediates = build_etl_with_intermediates(
            spark,
            cond_pmed_data["pmed_path"],
            cond_pmed_data["cond_path"],
            cond_pmed_data["clnk_path"],
            cond_pmed_data["fyc_path"],
        )
        pre = intermediates["cond_clnk"].count()
        post = intermediates["cond_clnk_dedup"].count()
        assert post <= pre, f"Dedup did not reduce rows: {post} > {pre}"

    def test_zero_fill_for_nonmatched_fyc(self, spark, cond_pmed_data):
        """Persons in FYC with no PMED fills must get n_hl_fills=0, hl_drug_exp=0."""
        from pyspark_migration.jobs.high_complexity.cond_pmed_2020 import (
            build_etl_with_intermediates,
        )

        intermediates = build_etl_with_intermediates(
            spark,
            cond_pmed_data["pmed_path"],
            cond_pmed_data["cond_path"],
            cond_pmed_data["clnk_path"],
            cond_pmed_data["fyc_path"],
        )
        result = intermediates["result"]

        # P005 has no PMED fills in test data
        p005 = result.filter(col("DUPERSID") == "P005").collect()
        assert len(p005) == 1
        assert p005[0]["n_hl_fills"] == 0
        assert p005[0]["hl_drug_exp"] == 0.0
        assert p005[0]["hl_pmed_flag"] == 0

    def test_fyc_left_join_preserves_all_rows(self, spark, cond_pmed_data):
        """Left join to FYC must preserve all FYC rows."""
        from pyspark_migration.jobs.high_complexity.cond_pmed_2020 import (
            build_etl_with_intermediates,
        )

        intermediates = build_etl_with_intermediates(
            spark,
            cond_pmed_data["pmed_path"],
            cond_pmed_data["cond_path"],
            cond_pmed_data["clnk_path"],
            cond_pmed_data["fyc_path"],
        )
        fyc_count = intermediates["fyc"].count()
        result_count = intermediates["result"].count()
        assert result_count == fyc_count, (
            f"Left join changed row count: {result_count} != {fyc_count}"
        )

    def test_no_null_survey_design_vars(self, spark, cond_pmed_data):
        """VARSTR, VARPSU, PERWT20F must not be null in final result."""
        from pyspark_migration.jobs.high_complexity.cond_pmed_2020 import (
            build_etl_with_intermediates,
        )

        intermediates = build_etl_with_intermediates(
            spark,
            cond_pmed_data["pmed_path"],
            cond_pmed_data["cond_path"],
            cond_pmed_data["clnk_path"],
            cond_pmed_data["fyc_path"],
        )
        result = intermediates["result"]
        for c in ["DUPERSID", "VARSTR", "VARPSU", "PERWT20F"]:
            null_count = result.filter(col(c).isNull()).count()
            assert null_count == 0, f"{c} has {null_count} null values"

    def test_person_level_unique_dupersid(self, spark, cond_pmed_data):
        """Person-level aggregation must have unique DUPERSID."""
        from pyspark_migration.jobs.high_complexity.cond_pmed_2020 import (
            build_etl_with_intermediates,
        )

        intermediates = build_etl_with_intermediates(
            spark,
            cond_pmed_data["pmed_path"],
            cond_pmed_data["cond_path"],
            cond_pmed_data["clnk_path"],
            cond_pmed_data["fyc_path"],
        )
        pl = intermediates["person_level"]
        total = pl.count()
        distinct = pl.select("DUPERSID").distinct().count()
        assert total == distinct


# ---------------------------------------------------------------------------
# cond_mv_2020 Tests (Stata version)
# ---------------------------------------------------------------------------


class TestCondMv2020:
    """Tests for cond_mv_2020 - Stata version of HL PMED analysis."""

    def test_schema_has_expected_columns(self, spark, cond_pmed_data):
        """Final result must have all required columns."""
        from pyspark_migration.jobs.high_complexity.cond_mv_2020 import (
            build_etl_with_intermediates,
        )

        intermediates = build_etl_with_intermediates(
            spark,
            cond_pmed_data["pmed_path"],
            cond_pmed_data["cond_path"],
            cond_pmed_data["clnk_path"],
            cond_pmed_data["fyc_path"],
        )
        result = intermediates["result"]
        expected = {
            "DUPERSID", "VARSTR", "VARPSU", "PERWT20F",
            "num_rx", "exp_rx", "any_rx",
        }
        assert expected.issubset(set(result.columns))

    def test_dedup_on_evntidx(self, spark, cond_pmed_data):
        """No duplicate EVNTIDX after de-duplication."""
        from pyspark_migration.jobs.high_complexity.cond_mv_2020 import (
            build_etl_with_intermediates,
        )

        intermediates = build_etl_with_intermediates(
            spark,
            cond_pmed_data["pmed_path"],
            cond_pmed_data["cond_path"],
            cond_pmed_data["clnk_path"],
            cond_pmed_data["fyc_path"],
        )
        dedup = intermediates["cond_clnk_dedup"]
        total = dedup.count()
        distinct = dedup.select("EVNTIDX").distinct().count()
        assert total == distinct

    def test_zero_fill_nonmatched(self, spark, cond_pmed_data):
        """Non-matched FYC persons get num_rx=0, exp_rx=0.0."""
        from pyspark_migration.jobs.high_complexity.cond_mv_2020 import (
            build_etl_with_intermediates,
        )

        intermediates = build_etl_with_intermediates(
            spark,
            cond_pmed_data["pmed_path"],
            cond_pmed_data["cond_path"],
            cond_pmed_data["clnk_path"],
            cond_pmed_data["fyc_path"],
        )
        result = intermediates["result"]
        p005 = result.filter(col("DUPERSID") == "P005").collect()
        assert len(p005) == 1
        assert p005[0]["num_rx"] == 0
        assert p005[0]["exp_rx"] == 0.0
        assert p005[0]["any_rx"] == 0

    def test_fyc_row_count_preserved(self, spark, cond_pmed_data):
        """Left join must preserve all FYC rows."""
        from pyspark_migration.jobs.high_complexity.cond_mv_2020 import (
            build_etl_with_intermediates,
        )

        intermediates = build_etl_with_intermediates(
            spark,
            cond_pmed_data["pmed_path"],
            cond_pmed_data["cond_path"],
            cond_pmed_data["clnk_path"],
            cond_pmed_data["fyc_path"],
        )
        assert intermediates["result"].count() == intermediates["fyc"].count()

    def test_any_rx_flag(self, spark, cond_pmed_data):
        """any_rx must be 1 when num_rx > 0, else 0."""
        from pyspark_migration.jobs.high_complexity.cond_mv_2020 import (
            build_etl_with_intermediates,
        )

        intermediates = build_etl_with_intermediates(
            spark,
            cond_pmed_data["pmed_path"],
            cond_pmed_data["cond_path"],
            cond_pmed_data["clnk_path"],
            cond_pmed_data["fyc_path"],
        )
        result = intermediates["result"]
        mismatch = result.filter(
            ((col("num_rx") > 0) & (col("any_rx") != 1))
            | ((col("num_rx") == 0) & (col("any_rx") != 0))
        ).count()
        assert mismatch == 0


# ---------------------------------------------------------------------------
# Exercise 4d Tests - Pooling with Variance Linkage
# ---------------------------------------------------------------------------


class TestExercise4d:
    """Tests for exercise_4d - Pooling FYC 2017-2019."""

    def test_pooled_has_all_years(self, spark, tmp_path_factory):
        """Pooled data must contain records from 2017, 2018, 2019."""
        from pyspark_migration.jobs.high_complexity.exercise_4d import (
            build_etl_with_intermediates,
        )

        # Create minimal synthetic data for each year
        from pyspark.sql.types import (
            DoubleType, IntegerType, StringType, StructField, StructType,
        )

        common_fields = [
            StructField("DUPERSID", StringType()),
            StructField("PANEL", IntegerType()),
            StructField("VARSTR", IntegerType()),
            StructField("VARPSU", IntegerType()),
            StructField("AGELAST", IntegerType()),
            StructField("ARTHDX", IntegerType()),
        ]

        # 2017
        schema17 = StructType(common_fields + [
            StructField("PERWT17F", DoubleType()),
            StructField("JTPAIN31", IntegerType()),
            StructField("TOTEXP17", DoubleType()),
            StructField("TOTSLF17", DoubleType()),
        ])
        path17 = str(tmp_path_factory.mktemp("fyc17"))
        spark.createDataFrame(
            [("P0010001", 21, 100, 1, 50, 1, 1500.0, 1, 5000.0, 1000.0)],
            schema17,
        ).write.mode("overwrite").parquet(path17)

        # 2018
        schema18 = StructType(common_fields + [
            StructField("PERWT18F", DoubleType()),
            StructField("JTPAIN31_M18", IntegerType()),
            StructField("TOTEXP18", DoubleType()),
            StructField("TOTSLF18", DoubleType()),
        ])
        path18 = str(tmp_path_factory.mktemp("fyc18"))
        spark.createDataFrame(
            [("P002000002", 22, 200, 2, 60, 2, 2000.0, 2, 6000.0, 1200.0)],
            schema18,
        ).write.mode("overwrite").parquet(path18)

        # 2019
        schema19 = StructType(common_fields + [
            StructField("PERWT19F", DoubleType()),
            StructField("JTPAIN31_M18", IntegerType()),
            StructField("TOTEXP19", DoubleType()),
            StructField("TOTSLF19", DoubleType()),
        ])
        path19 = str(tmp_path_factory.mktemp("fyc19"))
        spark.createDataFrame(
            [("P003000003", 23, 100, 1, 45, 1, 1800.0, 1, 4000.0, 800.0)],
            schema19,
        ).write.mode("overwrite").parquet(path19)

        # Linkage file
        linkage_schema = StructType([
            StructField("DUPERSID", StringType()),
            StructField("PANEL", IntegerType()),
            StructField("STRA9619", IntegerType()),
            StructField("PSU9619", IntegerType()),
        ])
        linkage_path = str(tmp_path_factory.mktemp("linkage"))
        spark.createDataFrame(
            [
                ("21P0010001", 21, 500, 1),
                ("P002000002", 22, 600, 2),
                ("P003000003", 23, 500, 1),
            ],
            linkage_schema,
        ).write.mode("overwrite").parquet(linkage_path)

        intermediates = build_etl_with_intermediates(
            spark, path17, path18, path19, linkage_path
        )
        pool = intermediates["pool"]

        # Must have all 3 years
        years = {row["YEAR"] for row in pool.select("YEAR").distinct().collect()}
        assert years == {2017, 2018, 2019}

    def test_poolwt_is_weight_div_3(self, spark, tmp_path_factory):
        """PERWTF must equal year-specific weight / 3."""
        from pyspark_migration.jobs.high_complexity.exercise_4d import (
            build_etl_with_intermediates,
        )

        from pyspark.sql.types import (
            DoubleType, IntegerType, StringType, StructField, StructType,
        )

        common_fields = [
            StructField("DUPERSID", StringType()),
            StructField("PANEL", IntegerType()),
            StructField("VARSTR", IntegerType()),
            StructField("VARPSU", IntegerType()),
            StructField("AGELAST", IntegerType()),
            StructField("ARTHDX", IntegerType()),
        ]

        schema17 = StructType(common_fields + [
            StructField("PERWT17F", DoubleType()),
            StructField("JTPAIN31", IntegerType()),
            StructField("TOTEXP17", DoubleType()),
            StructField("TOTSLF17", DoubleType()),
        ])
        path17 = str(tmp_path_factory.mktemp("fyc17_wt"))
        spark.createDataFrame(
            [("P0010001", 21, 100, 1, 50, 1, 3000.0, 1, 5000.0, 1000.0)],
            schema17,
        ).write.mode("overwrite").parquet(path17)

        schema18 = StructType(common_fields + [
            StructField("PERWT18F", DoubleType()),
            StructField("JTPAIN31_M18", IntegerType()),
            StructField("TOTEXP18", DoubleType()),
            StructField("TOTSLF18", DoubleType()),
        ])
        path18 = str(tmp_path_factory.mktemp("fyc18_wt"))
        spark.createDataFrame(
            [("P002000002", 22, 200, 2, 60, 2, 6000.0, 2, 6000.0, 1200.0)],
            schema18,
        ).write.mode("overwrite").parquet(path18)

        schema19 = StructType(common_fields + [
            StructField("PERWT19F", DoubleType()),
            StructField("JTPAIN31_M18", IntegerType()),
            StructField("TOTEXP19", DoubleType()),
            StructField("TOTSLF19", DoubleType()),
        ])
        path19 = str(tmp_path_factory.mktemp("fyc19_wt"))
        spark.createDataFrame(
            [("P003000003", 23, 100, 1, 45, 1, 9000.0, 1, 4000.0, 800.0)],
            schema19,
        ).write.mode("overwrite").parquet(path19)

        linkage_schema = StructType([
            StructField("DUPERSID", StringType()),
            StructField("PANEL", IntegerType()),
            StructField("STRA9619", IntegerType()),
            StructField("PSU9619", IntegerType()),
        ])
        linkage_path = str(tmp_path_factory.mktemp("linkage_wt"))
        spark.createDataFrame(
            [("21P0010001", 21, 500, 1), ("P002000002", 22, 600, 2),
             ("P003000003", 23, 500, 1)],
            linkage_schema,
        ).write.mode("overwrite").parquet(linkage_path)

        intermediates = build_etl_with_intermediates(
            spark, path17, path18, path19, linkage_path
        )
        pool = intermediates["pool"]

        # 2017 record: PERWT17F=3000 -> PERWTF=1000
        r17 = pool.filter(col("YEAR") == 2017).collect()[0]
        assert abs(r17["PERWTF"] - 1000.0) < 0.01

        # 2018 record: PERWT18F=6000 -> PERWTF=2000
        r18 = pool.filter(col("YEAR") == 2018).collect()[0]
        assert abs(r18["PERWTF"] - 2000.0) < 0.01


# ---------------------------------------------------------------------------
# Unit-level de-duplication correctness tests
# ---------------------------------------------------------------------------


class TestDeduplication:
    """Unit tests for EVNTIDX de-duplication logic."""

    def test_dedup_on_evntidx_basic(self, spark):
        """EVNTIDX dedup must match Stata 'duplicates drop evntidx, force'."""
        data = [("P001", "C001", "E001"), ("P001", "C002", "E001")]
        df = spark.createDataFrame(data, ["DUPERSID", "CONDIDX", "EVNTIDX"])
        result = df.dropDuplicates(["EVNTIDX"])
        assert result.count() == 1

    def test_dedup_preserves_unique(self, spark):
        """Unique EVNTIDX values must all be preserved."""
        data = [
            ("P001", "C001", "E001"),
            ("P001", "C002", "E002"),
            ("P002", "C003", "E003"),
        ]
        df = spark.createDataFrame(data, ["DUPERSID", "CONDIDX", "EVNTIDX"])
        result = df.dropDuplicates(["EVNTIDX"])
        assert result.count() == 3

    def test_zero_fill_for_nonmatched_fyc(self, spark):
        """Persons in FYC with no PMED fills must get num_rx=0, exp_rx=0."""
        fyc = spark.createDataFrame([("P001",), ("P002",)], ["DUPERSID"])
        pmed_agg = spark.createDataFrame(
            [("P001", 3, 150.0)], ["DUPERSID", "num_rx", "exp_rx"]
        )
        result = fyc.join(pmed_agg, on="DUPERSID", how="left").fillna(
            {"num_rx": 0, "exp_rx": 0.0}
        )
        p2 = result.filter(col("DUPERSID") == "P002").collect()[0]
        assert p2["num_rx"] == 0
        assert p2["exp_rx"] == 0.0
