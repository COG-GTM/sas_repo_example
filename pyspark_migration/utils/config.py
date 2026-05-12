"""Configuration for MEPS PySpark migration."""

import os

# Base data path - configurable via environment variable
DATA_PATH = os.environ.get("MEPS_DATA_PATH", "data/meps")

# Output path for processed Parquet files
OUTPUT_PATH = os.environ.get("MEPS_OUTPUT_PATH", "data/processed")

# MEPS file name mappings (file code -> description)
MEPS_FILES = {
    # 2015 FYC
    "h181": {"year": 2015, "type": "FYC", "description": "2015 Full-Year Consolidated"},
    # 2016 FYC
    "h192": {"year": 2016, "type": "FYC", "description": "2016 Full-Year Consolidated"},
    # 2016 RX event
    "h188a": {"year": 2016, "type": "PMED", "description": "2016 Prescribed Medicines"},
    # 2017 FYC
    "h201": {"year": 2017, "type": "FYC", "description": "2017 Full-Year Consolidated"},
    # 2018 FYC
    "h209": {"year": 2018, "type": "FYC", "description": "2018 Full-Year Consolidated"},
    # 2019 FYC
    "h216": {"year": 2019, "type": "FYC", "description": "2019 Full-Year Consolidated"},
    # 2020 FYC
    "h224": {"year": 2020, "type": "FYC", "description": "2020 Full-Year Consolidated"},
    # 2020 PMED
    "h220a": {"year": 2020, "type": "PMED", "description": "2020 Prescribed Medicines"},
    # 2020 Conditions
    "h222": {"year": 2020, "type": "Conditions", "description": "2020 Conditions"},
    # 2020 CLNK
    "h220if1": {"year": 2020, "type": "CLNK", "description": "2020 Condition-Event Link"},
    # Pooled Linkage Variance file
    "h36u19": {"year": None, "type": "Variance", "description": "1996-2019 Pooled Linkage Variance"},
}

# Survey design variable names
SURVEY_VARS = {
    "strata": "VARSTR",
    "cluster": "VARPSU",
}

# Year-specific weight variable pattern
def get_weight_var(year):
    """Return the person-level weight variable name for a given year."""
    yy = str(year)[-2:]
    return f"PERWT{yy}F"
