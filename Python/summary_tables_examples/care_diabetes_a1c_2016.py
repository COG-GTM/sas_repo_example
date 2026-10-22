# -----------------------------------------------------------------------------
# Example code to replicate estimates from the MEPS-HC Data Tools summary tables
#
# Accessibility and quality of care: Diabetes Care, 2016
#
# Diabetes care survey (DCS):
#  - Number/percent of adults with diabetes receiving hemoglobin A1c blood test
#  - By race/ethnicity
#
# Input file: C:/MEPS/h192.ssp (2016 full-year consolidated)
#
# Python port of SAS/summary_tables_examples/care_diabetes_a1c_2016.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import MEPS_DIR, apply_format, crosstab_list, read_meps, svy_freq  # noqa: E402

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)

# Load FYC file (SAS transport file) ------------------------------------------

h192 = read_meps(f"{MEPS_DIR}/h192.ssp")

# Define variables ------------------------------------------------------------

MEPS = h192.copy()

# Define domain and adjust weights so observations are not dropped
MEPS["domain"] = (MEPS["DIABW16F"] > 0).astype(int)
MEPS.loc[MEPS["domain"] == 0, "DIABW16F"] = 1

# Race/ethnicity
# 1996-2002: race/ethnicity variable based on RACETHNX (see documentation)
# 2002-2011: race/ethnicity variable based on RACETHNX and RACEX:
#   hisp   = (RACETHNX = 1);
#   white  = (RACETHNX = 4 & RACEX = 1);
#   black  = (RACETHNX = 2);
#   native = (RACETHNX ge 3 and RACEX in (3,6));
#   asian  = (RACETHNX ge 3 and RACEX in (4,5)))
# For 2012 and later, use RACETHX and RACEV1X:
MEPS["hisp"] = (MEPS["RACETHX"] == 1).astype(int)
MEPS["white"] = (MEPS["RACETHX"] == 2).astype(int)
MEPS["black"] = (MEPS["RACETHX"] == 3).astype(int)
MEPS["native"] = ((MEPS["RACETHX"] > 3) & MEPS["RACEV1X"].isin([3, 6])).astype(int)
MEPS["asian"] = ((MEPS["RACETHX"] > 3) & MEPS["RACEV1X"].isin([4, 5])).astype(int)
MEPS["race"] = (
    1 * MEPS["hisp"] + 2 * MEPS["white"] + 3 * MEPS["black"] + 4 * MEPS["native"] + 5 * MEPS["asian"]
)

# Formats (PROC FORMAT) -------------------------------------------------------

diab_a1c = [
    (-9, -7, "Don't know/Non-response"),
    (-1, -1, "Inapplicable"),
    (0, 0, "Did not have measurement"),
    (96, 96, "Did not have measurement"),
    (1, 95, "Had measurement"),
]
race = {
    1: "1 Hispanic",
    2: "2 White",
    3: "3 Black",
    4: "4 Amer. Indian, AK Native, or mult. races",
    5: "5 Asian, Hawaiian, or Pacific Islander",
}

# QC new variables
qc = MEPS.assign(race=apply_format(MEPS["race"], race))
print(crosstab_list(qc, ["RACETHX", "RACEV1X", "hisp", "white", "black", "native", "asian", "race"]), "\n")

MEPS["DSA1C53"] = apply_format(MEPS["DSA1C53"], diab_a1c)
MEPS["race"] = apply_format(MEPS["race"], race)

# Calculate estimates using survey procedures ---------------------------------
#  - use DIABW16F weight variable, since outcome variable comes from DCS
#  - TABLES domain*race*DSA1C53 / row

out = svy_freq(
    MEPS, "DSA1C53", weight="DIABW16F", strata="VARSTR", psu="VARPSU", domain=["domain", "race"]
)

# Adults with diabetes with hemoglobin A1C measurement in 2016, by race
print("Adults with diabetes with hemoglobin A1C measurement in 2016, by race")
keep = (out["domain"] == 1) & (out["DSA1C53"] != "Missing") & (out["race"] != "Missing")
print(out[keep].drop(columns="domain").to_string(index=False))
