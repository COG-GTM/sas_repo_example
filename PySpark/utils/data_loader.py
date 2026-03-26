"""
MEPS Data Loading Utilities for PySpark

Functions for loading MEPS data files in various formats (.sas7bdat, .ssp XPORT)
and downloading files from the MEPS website.

Replaces SAS patterns:
  - DATA work.file; SET "path/file.sas7bdat"; RUN;
  - PROC XCOPY for .ssp files
  - PROC HTTP for downloading MEPS files
"""

import os
import zipfile
import io

import requests
import pandas as pd
from pyspark.sql import SparkSession, DataFrame


def get_spark(app_name: str = "MEPS") -> SparkSession:
    """Create or get an existing SparkSession.

    Returns:
        SparkSession configured for MEPS analysis.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )


def load_sas7bdat(spark: SparkSession, filepath: str) -> DataFrame:
    """Load a .sas7bdat file (2017+ MEPS data) into a PySpark DataFrame.

    Uses pandas as an intermediate step to read the SAS7BDAT format,
    then converts to a Spark DataFrame.

    Args:
        spark: Active SparkSession.
        filepath: Path to the .sas7bdat file.

    Returns:
        PySpark DataFrame containing the data.

    Replaces SAS:
        DATA work.h209; SET "C:/MEPS/h209.sas7bdat"; RUN;
    """
    pdf = pd.read_sas(filepath, format="sas7bdat", encoding="utf-8")
    # Decode any remaining byte-string columns
    for col in pdf.select_dtypes(include=["object"]).columns:
        pdf[col] = pdf[col].apply(
            lambda x: x.decode("utf-8", errors="replace")
            if isinstance(x, bytes) else x
        )
    return spark.createDataFrame(pdf)


def load_ssp(spark: SparkSession, filepath: str) -> DataFrame:
    """Load a .ssp (SAS XPORT) file (1996-2016 MEPS data) into a PySpark DataFrame.

    Uses pandas as an intermediate step to read the XPORT format,
    then converts to a Spark DataFrame.

    Args:
        spark: Active SparkSession.
        filepath: Path to the .ssp XPORT file.

    Returns:
        PySpark DataFrame containing the data.

    Replaces SAS:
        FILENAME in_file "C:/MEPS/h192.ssp";
        PROC XCOPY in=in_file out=WORK IMPORT; RUN;
    """
    pdf = pd.read_sas(filepath, format="xport", encoding="utf-8")
    for col in pdf.select_dtypes(include=["object"]).columns:
        pdf[col] = pdf[col].apply(
            lambda x: x.decode("utf-8", errors="replace")
            if isinstance(x, bytes) else x
        )
    return spark.createDataFrame(pdf)


def download_meps_file(
    meps_file: str,
    meps_url: str = "https://meps.ahrq.gov/mepsweb/data_files/pufs",
    local_dir: str = ".",
) -> str:
    """Download and unzip a MEPS data file from the AHRQ website.

    Args:
        meps_file: Name of the MEPS file (e.g., "h224").
        meps_url: Base URL for MEPS PUF downloads.
        local_dir: Local directory to save the extracted file.

    Returns:
        Path to the downloaded/extracted file.

    Replaces SAS:
        PROC HTTP url="https://meps.ahrq.gov/.../h224ssp.zip"
            out="C:/MEPS/h224.ssp"; RUN;
    """
    os.makedirs(local_dir, exist_ok=True)

    # Try .sas7bdat (zip) first, then .ssp (zip)
    for ext, suffix in [("sas7bdat", "v9"), ("ssp", "ssp")]:
        zip_url = f"{meps_url}/{meps_file}{suffix}.zip"
        try:
            response = requests.get(zip_url, timeout=120)
            response.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                zf.extractall(local_dir)
            extracted = os.path.join(local_dir, f"{meps_file}.{ext}")
            if os.path.exists(extracted):
                return extracted
            # Find the extracted file
            for name in zf.namelist():
                full_path = os.path.join(local_dir, name)
                if os.path.exists(full_path):
                    return full_path
        except (requests.RequestException, zipfile.BadZipFile):
            continue

    raise FileNotFoundError(
        f"Could not download MEPS file '{meps_file}' from {meps_url}"
    )
