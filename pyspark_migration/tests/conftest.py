"""Pytest configuration and shared fixtures for MEPS PySpark Migration tests.

Provides a shared SparkSession and synthetic test data fixtures that
mirror the structure of real MEPS data files.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


@pytest.fixture(scope="session")
def spark():
    """Create a shared SparkSession for all tests."""
    session = (
        SparkSession.builder
        .appName("MEPS_PySpark_Migration_Tests")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-test")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def tmp_parquet_dir(tmp_path_factory):
    """Create a temporary directory for Parquet output."""
    return str(tmp_path_factory.mktemp("parquet_output"))


# ---------------------------------------------------------------------------
# Synthetic MEPS data fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def h192_data(spark, tmp_path_factory):
    """Synthetic H192 (2016 FYC) data for testing."""
    path = str(tmp_path_factory.mktemp("h192"))
    data = [
        ("P001", 5000.0, 45, 45, 45, 100, 1, 1500.0, 1, 1, 1, 1, 1500.0),
        ("P002", 0.0, 70, 70, 70, 200, 2, 2000.0, 2, 2, 2, 2, 0.0),
        ("P003", 12000.0, 30, 30, 30, 100, 1, 1800.0, 3, 3, 3, 3, 3600.0),
        ("P004", 800.0, 80, 80, 80, 200, 2, 500.0, 4, 4, 4, 4, 240.0),
        ("P005", 0.0, 50, 50, 50, 100, 1, 0.0, 5, 5, 5, 5, 0.0),
    ]
    schema = StructType([
        StructField("DUPERSID", StringType()),
        StructField("TOTEXP16", DoubleType()),
        StructField("AGE16X", IntegerType()),
        StructField("AGE42X", IntegerType()),
        StructField("AGE31X", IntegerType()),
        StructField("VARSTR", IntegerType()),
        StructField("VARPSU", IntegerType()),
        StructField("PERWT16F", DoubleType()),
        StructField("AGELAST", IntegerType()),
        StructField("INSURC16", IntegerType()),
        StructField("INSCOV16", IntegerType()),
        StructField("POVCAT16", IntegerType()),
        StructField("TOTSLF16", DoubleType()),
    ])
    df = spark.createDataFrame(data, schema)
    df.write.mode("overwrite").parquet(path)
    return path


@pytest.fixture(scope="session")
def h181_data(spark, tmp_path_factory):
    """Synthetic H181 (2015 FYC) data for testing."""
    path = str(tmp_path_factory.mktemp("h181"))
    data = [
        ("P001", "D001", "F001", 1200.0, 100, 1, 3000.0, 500.0, 200.0, 1500.0,
         100.0, 50.0, 400.0, 50.0, 50.0, 50.0, 50.0, 50.0, 45, 45, 45, 1500.0,
         5000.0, 1, 1),
        ("P002", "D001", "F001", 800.0, 200, 2, 0.0, 0.0, 0.0, 0.0,
         0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 70, 70, 70, 2000.0,
         3000.0, 2, 2),
        ("P003", "D002", "F001", 500.0, 100, 1, 8000.0, 1000.0, 500.0, 3000.0,
         200.0, 100.0, 1500.0, 100.0, 200.0, 100.0, 200.0, 100.0, 30, 30, 30, 1800.0,
         6000.0, 3, 3),
    ]
    schema = StructType([
        StructField("DUPERSID", StringType()),
        StructField("DUID", StringType()),
        StructField("CPSFAMID", StringType()),
        StructField("FAMWT15C", DoubleType()),
        StructField("VARSTR", IntegerType()),
        StructField("VARPSU", IntegerType()),
        StructField("TOTEXP15", DoubleType()),
        StructField("IPDEXP15", DoubleType()),
        StructField("IPFEXP15", DoubleType()),
        StructField("OBVEXP15", DoubleType()),
        StructField("OPDEXP15", DoubleType()),
        StructField("OPFEXP15", DoubleType()),
        StructField("RXEXP15", DoubleType()),
        StructField("DVTEXP15", DoubleType()),
        StructField("ERDEXP15", DoubleType()),
        StructField("ERFEXP15", DoubleType()),
        StructField("HHAEXP15", DoubleType()),
        StructField("HHNEXP15", DoubleType()),
        StructField("AGE15X", IntegerType()),
        StructField("AGE42X", IntegerType()),
        StructField("AGE31X", IntegerType()),
        StructField("PERWT15F", DoubleType()),
        StructField("TTLP15X", DoubleType()),
        StructField("INSCOV15", IntegerType()),
        StructField("POVCAT15", IntegerType()),
    ])
    # Need to add missing columns: OTHEXP15, VISEXP15, TOTSLF15, AGELAST
    from pyspark.sql.functions import lit
    df = spark.createDataFrame(data, schema)
    df = df.withColumn("OTHEXP15", lit(0.0))
    df = df.withColumn("VISEXP15", lit(0.0))
    df = df.withColumn("TOTSLF15", df["TOTEXP15"] * 0.3)
    df = df.withColumn("AGELAST", df["AGE15X"])
    df.write.mode("overwrite").parquet(path)
    return path


@pytest.fixture(scope="session")
def h216_data(spark, tmp_path_factory):
    """Synthetic H216 (2019 FYC) data for testing care_access_2019."""
    path = str(tmp_path_factory.mktemp("h216"))
    data = [
        ("P001", 1, 2, 1, 1, 1, 100, 1, 1500.0),
        ("P002", 2, 1, 2, 1, 0, 200, 2, 2000.0),
        ("P003", 1, 1, 1, 1, 1, 100, 1, 0.0),
        ("P004", 2, 2, 2, 0, 1, 200, 2, 1800.0),
        ("P005", -1, -1, -1, 3, 0, 100, 1, 500.0),
    ]
    schema = StructType([
        StructField("DUPERSID", StringType()),
        StructField("AFRDCA42", IntegerType()),
        StructField("AFRDDN42", IntegerType()),
        StructField("AFRDPM42", IntegerType()),
        StructField("POVCAT19", IntegerType()),
        StructField("ACCELI42", IntegerType()),
        StructField("VARSTR", IntegerType()),
        StructField("VARPSU", IntegerType()),
        StructField("PERWT19F", DoubleType()),
    ])
    df = spark.createDataFrame(data, schema)
    df.write.mode("overwrite").parquet(path)
    return path


@pytest.fixture(scope="session")
def rx_h188a_data(spark, tmp_path_factory):
    """Synthetic H188A (2016 RX event) data for pmed_prescribed_drug_2016."""
    path = str(tmp_path_factory.mktemp("h188a"))
    data = [
        ("P001", 100, 1, 1500.0, "ATORVASTATIN CALCIUM", 45.50),
        ("P001", 100, 1, 1500.0, "ATORVASTATIN CALCIUM", 45.50),
        ("P001", 100, 1, 1500.0, "LISINOPRIL", 12.00),
        ("P002", 200, 2, 2000.0, "METFORMIN HCL", 8.25),
        ("P002", 200, 2, 2000.0, "METFORMIN HCL", 8.25),
        ("P003", 100, 1, 1800.0, "ATORVASTATIN CALCIUM", 50.00),
    ]
    schema = StructType([
        StructField("DUPERSID", StringType()),
        StructField("VARSTR", IntegerType()),
        StructField("VARPSU", IntegerType()),
        StructField("PERWT16F", DoubleType()),
        StructField("RXDRGNAM", StringType()),
        StructField("RXXP16X", DoubleType()),
    ])
    df = spark.createDataFrame(data, schema)
    df.write.mode("overwrite").parquet(path)
    return path


@pytest.fixture(scope="session")
def cond_pmed_data(spark, tmp_path_factory):
    """Synthetic data for cond_pmed_2020 / cond_mv_2020 high-complexity tests.

    Returns dict with paths to: pmed, cond, clnk, fyc
    """
    base = tmp_path_factory.mktemp("cond_pmed")

    # PMED (h220a) - Rx fill level
    pmed_path = str(base / "pmed")
    pmed_data = [
        ("P001", "D001", "RX001", "E001", "ATORVASTATIN CALCIUM", 45.50),
        ("P001", "D001", "RX002", "E001", "ATORVASTATIN CALCIUM", 45.50),
        ("P001", "D002", "RX003", "E002", "ROSUVASTATIN CALCIUM", 30.00),
        ("P002", "D003", "RX004", "E003", "SIMVASTATIN", 22.00),
        ("P003", "D004", "RX005", "E004", "METFORMIN HCL", 8.00),
    ]
    pmed_schema = StructType([
        StructField("DUPERSID", StringType()),
        StructField("DRUGIDX", StringType()),
        StructField("RXRECIDX", StringType()),
        StructField("LINKIDX", StringType()),
        StructField("RXDRGNAM", StringType()),
        StructField("RXXP20X", DoubleType()),
    ])
    spark.createDataFrame(pmed_data, pmed_schema).write.mode("overwrite").parquet(pmed_path)

    # Conditions (h222) - condition level
    cond_path = str(base / "cond")
    cond_data = [
        ("P001", "C001", "E78", "END010", "", ""),
        ("P001", "C002", "E78", "END010", "", ""),  # Duplicate HL for P001
        ("P002", "C003", "E78", "END010", "CIR007", ""),
        ("P003", "C004", "E11", "END003", "", ""),  # Not hyperlipidemia
        ("P004", "C005", "E78", "", "END010", ""),  # END010 in CCSR2X
    ]
    cond_schema = StructType([
        StructField("DUPERSID", StringType()),
        StructField("CONDIDX", StringType()),
        StructField("ICD10CDX", StringType()),
        StructField("CCSR1X", StringType()),
        StructField("CCSR2X", StringType()),
        StructField("CCSR3X", StringType()),
    ])
    spark.createDataFrame(cond_data, cond_schema).write.mode("overwrite").parquet(cond_path)

    # CLNK (h220if1) - condition-event link
    clnk_path = str(base / "clnk")
    clnk_data = [
        ("P001", "C001", "E001", 8),
        ("P001", "C002", "E001", 8),  # Both HL conditions link to same event
        ("P001", "C001", "E002", 8),
        ("P002", "C003", "E003", 8),
        ("P003", "C004", "E004", 8),  # Non-HL condition
        ("P004", "C005", "E005", 8),  # No matching PMED for E005
    ]
    clnk_schema = StructType([
        StructField("DUPERSID", StringType()),
        StructField("CONDIDX", StringType()),
        StructField("EVNTIDX", StringType()),
        StructField("EVENTYPE", IntegerType()),
    ])
    spark.createDataFrame(clnk_data, clnk_schema).write.mode("overwrite").parquet(clnk_path)

    # FYC (h224) - person level
    fyc_path = str(base / "fyc")
    fyc_data = [
        ("P001", 55, 1, 1, 5, 100, 1, 1500.0),
        ("P002", 62, 2, 1, 3, 200, 2, 2000.0),
        ("P003", 40, 1, 2, 4, 100, 1, 1800.0),
        ("P004", 70, 2, 1, 2, 200, 2, 1200.0),
        ("P005", 30, 1, 2, 5, 100, 1, 900.0),  # No PMED fills
    ]
    fyc_schema = StructType([
        StructField("DUPERSID", StringType()),
        StructField("AGELAST", IntegerType()),
        StructField("SEX", IntegerType()),
        StructField("CHOLDX", IntegerType()),
        StructField("POVCAT20", IntegerType()),
        StructField("VARSTR", IntegerType()),
        StructField("VARPSU", IntegerType()),
        StructField("PERWT20F", DoubleType()),
    ])
    spark.createDataFrame(fyc_data, fyc_schema).write.mode("overwrite").parquet(fyc_path)

    return {
        "pmed_path": pmed_path,
        "cond_path": cond_path,
        "clnk_path": clnk_path,
        "fyc_path": fyc_path,
    }
