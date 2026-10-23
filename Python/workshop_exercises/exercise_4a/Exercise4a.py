"""Python port of SAS/workshop_exercises/exercise_4a/Exercise4a.sas

DESCRIPTION:  THIS PROGRAM ILLUSTRATES HOW TO POOL MEPS DATA FILES FROM DIFFERENT YEARS
              THE EXAMPLE USED IS POPULATION AGE 26-30 WHO ARE UNINSURED BUT HAVE HIGH INCOME

             DATA FROM 2015 AND 2016 ARE POOLED.

              VARIABLES WITH YEAR-SPECIFIC NAMES MUST BE RENAMED BEFORE COMBINING FILES.
              IN THIS PROGRAM THE INSURANCE COVERAGE VARIABLES 'INSCOV15' AND 'INSCOV16' ARE RENAMED TO 'INSCOV'.

             SEE HC-036 (1996-2015 POOLED ESTIMATION FILE) FOR
              INSTRUCTIONS ON POOOLING AND CONSIDERATIONS FOR VARIANCE
             ESTIMATION FOR PRE-2002 DATA.

INPUT FILE:     (1) <MEPS_DIR>/SAS/DATA/H192.SAS7BDAT (2016 FULL-YEAR FILE)
                (2) <MEPS_DIR>/SAS/DATA/H181.SAS7BDAT (2015 FULL-YEAR FILE)

;
IMPORTANT NOTES: Use the next 6 lines of code, if you want to specify an alternative destination for SAS log and
SAS procedure output.

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# PROC FORMAT
POVCAT = {1: "1 POOR/NEGATIVE", 2: "2 NEAR POOR", 3: "3 LOW INCOME", 4: "4 MIDDLE INCOME", 5: "5 HIGH INCOME"}
INSF = {1: "1 ANY PRIVATE", 2: "2 PUBLIC ONLY", 3: "3 UNINSURED"}
AGE = {slice(26, 30): "26-30", slice(0, 25): "0-25", slice(31, None): "31+"}

# Year 1 (2015) and year 2 (2016) FYC files, persons with positive weight -------------
YR1 = read_meps("h181", ["DUPERSID", "INSCOV15", "PERWT15F", "VARSTR", "VARPSU", "POVCAT15", "AGELAST", "TOTSLF15"])
YR1 = YR1[YR1.PERWT15F > 0]
YR2 = read_meps("h192", ["DUPERSID", "INSCOV16", "PERWT16F", "VARSTR", "VARPSU", "POVCAT16", "AGELAST", "TOTSLF16"])
YR2 = YR2[YR2.PERWT16F > 0]
print(freq_list(YR1[YR1.AGELAST.between(26, 30)], ["POVCAT15", "INSCOV15"]))
print(freq_list(YR2[YR2.AGELAST.between(26, 30)], ["POVCAT16", "INSCOV16"]))

# Rename year-specific variables and pool (SAS: SET YR1X YR2X) ------------------------
YR1X = YR1.rename(columns={"INSCOV15": "INSCOV", "PERWT15F": "PERWT", "POVCAT15": "POVCAT", "TOTSLF15": "TOTSLF"})
YR2X = YR2.rename(columns={"INSCOV16": "INSCOV", "PERWT16F": "PERWT", "POVCAT16": "POVCAT", "TOTSLF16": "TOTSLF"})
POOL = pd.concat([YR1X, YR2X], ignore_index=True)
POOL["POOLWT"] = POOL.PERWT / 2
POOL["SUBPOP"] = np.where(POOL.AGELAST.between(26, 30) & (POOL.POVCAT == 5) & (POOL.INSCOV == 3), 1, 2)

print(POOL.notna().sum().rename("N").to_frame().assign(NMISS=POOL.isna().sum()))
print(freq_list(POOL, ["SUBPOP"]))
print(freq_list(POOL.assign(AGELAST=sas_format(POOL.AGELAST, AGE)), ["SUBPOP", "AGELAST", "POVCAT", "INSCOV"]))

# Mean out-of-pocket expense, uninsured high-income persons aged 26-30 (DOMAIN SUBPOP('1'))
out = surveymeans(POOL, "TOTSLF", weight="POOLWT", strata="VARSTR", cluster="VARPSU", domain="SUBPOP")
show("Total amount paid by self/family, SUBPOP = 1 (pooled 2015-2016)", out[out.SUBPOP == 1][["VarName", "N", "Mean", "StdErr"]])
