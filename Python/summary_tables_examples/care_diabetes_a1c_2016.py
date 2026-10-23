"""Python port of SAS/summary_tables_examples/care_diabetes_a1c_2016.sas

Example code to replicate estimates from the MEPS-HC Data Tools summary tables

Accessibility and quality of care: Diabetes Care, 2016

Diabetes care survey (DCS):
 - Number/percent of adults with diabetes receiving hemoglobin A1c blood test
 - By race/ethnicity

Input file: <MEPS_DIR>/h192.ssp (2016 full-year consolidated)

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path


sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# Load FYC file (SAS: PROC XCOPY) -----------------------------------------------
h192 = read_meps("h192")

# Define variables ----------------------------------------------------------------
MEPS = h192.copy()
# Domain = persons in the diabetes care supplement; give everyone else weight 1
# so their PSUs stay in the variance calculation (SAS DOMAIN approach).
MEPS["domain"] = (MEPS.DIABW16F > 0).astype(int)
MEPS.loc[MEPS.domain == 0, "DIABW16F"] = 1

# 2002-2011: race/ethnicity variable based on RACETHNX and RACEX:
#   hisp   = (RACETHNX == 1)
#   white  = (RACETHNX == 4) & (RACEX == 1)
#   black  = (RACETHNX == 2)
#   native = (RACETHNX >= 3) & RACEX.isin([3, 6])
#   asian  = (RACETHNX >= 3) & RACEX.isin([4, 5])
#
# For 2012 and later, use RACETHX and RACEV1X:
MEPS["hisp"] = (MEPS.RACETHX == 1).astype(int)
MEPS["white"] = (MEPS.RACETHX == 2).astype(int)
MEPS["black"] = (MEPS.RACETHX == 3).astype(int)
MEPS["native"] = ((MEPS.RACETHX > 3) & MEPS.RACEV1X.isin([3, 6])).astype(int)
MEPS["asian"] = ((MEPS.RACETHX > 3) & MEPS.RACEV1X.isin([4, 5])).astype(int)
MEPS["race"] = 1 * MEPS.hisp + 2 * MEPS.white + 3 * MEPS.black + 4 * MEPS.native + 5 * MEPS.asian

# PROC FORMAT
diab_a1c = {(-9, -8, -7): "Don't know/Non-response", -1: "Inapplicable",
            (0, 96): "Did not have measurement", slice(1, 95): "Had measurement"}
race = {1: "1 Hispanic", 2: "2 White", 3: "3 Black", 4: "4 Amer. Indian, AK Native, or mult. races",
        5: "5 Asian, Hawaiian, or Pacific Islander"}

# QC check on new race variable
print(freq_list(MEPS, ["RACETHX", "RACEV1X", "hisp", "white", "black", "native", "asian", "race"]))

MEPS["DSA1C53"] = sas_format(MEPS.DSA1C53, diab_a1c)
MEPS["race"] = sas_format(MEPS.race, race, missing="Missing")

# Calculate estimates using survey procedures (PROC SURVEYFREQ ... / ROW) ---------
out = surveyfreq(MEPS, ["domain", "race", "DSA1C53"], weight="DIABW16F", strata="VARSTR", cluster="VARPSU",
                 missing=True)

# Adults with diabetes who had a hemoglobin A1c measurement, by race/ethnicity
show("Diabetes care: hemoglobin A1c measurement, by race/ethnicity (domain = 1)",
     out[out.domain == 1][["DSA1C53", "race", "Frequency", "WgtFreq", "StdDev", "RowPercent", "RowStdErr"]])
