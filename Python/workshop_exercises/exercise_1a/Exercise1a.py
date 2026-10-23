"""Python port of SAS/workshop_exercises/exercise_1a/Exercise1a.sas

DESCRIPTION:  THIS PROGRAM GENERATES THE FOLLOWING ESTIMATES ON NATIONAL HEALTH CARE EXPENSES, 2016:

               (1) OVERALL EXPENSES
               (2) PERCENTAGE OF PERSONS WITH AN EXPENSE
               (3) MEAN EXPENSE PER PERSON WITH AN EXPENSE


INPUT FILE:   <MEPS_DIR>/SAS/DATA/H192.SAS7BDAT (2016 FULL-YEAR FILE)

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
AGECAT = {1: "0-64", 2: "65+"}

# Load FYC file and define variables (SAS: DATA PUF192; SET CDATA.H192 (KEEP=...)) --
PUF192 = read_meps("h192", ["TOTEXP16", "AGE16X", "AGE42X", "AGE31X", "VARSTR", "VARPSU", "PERWT16F"])
PUF192["TOTAL"] = PUF192.TOTEXP16
PUF192["X_ANYSVCE"] = np.where(PUF192.TOTAL > 0, 1, 0)

# Age: use last available age (AGE16X, then AGE42X, then AGE31X)
PUF192["AGE"] = np.select([PUF192.AGE16X >= 0, PUF192.AGE42X >= 0, PUF192.AGE31X >= 0],
                          [PUF192.AGE16X, PUF192.AGE42X, PUF192.AGE31X], np.nan)
PUF192["AGECAT"] = np.select([PUF192.AGE.between(0, 64), PUF192.AGE > 64], [1, 2], np.nan)

# QC (PROC FREQ ... / LIST MISSING)
print(freq_list(PUF192.assign(TOTAL=sas_format(PUF192.TOTAL, {0: "0", slice(1e-12, None): ">0"})),
                ["X_ANYSVCE", "TOTAL"]))
print(freq_list(PUF192.assign(AGE=sas_format(PUF192.AGE, {slice(0, 64): "0-64", slice(65, None): "65+"},
                                             missing="ALL AGES")), ["AGECAT", "AGE"]))

# Percentage of persons with an expense, and overall expenses -----------------------
overall = surveymeans(PUF192, ["X_ANYSVCE", "TOTAL"], weight="PERWT16F", strata="VARSTR", cluster="VARPSU")
show("Percentage of persons with an expense", overall.iloc[[0]][["N", "SumWgt", "Mean", "StdErr", "Sum", "StdDev"]])
show("Overall expenses", overall.iloc[[1]][["N", "SumWgt", "Mean", "StdErr", "Sum", "StdDev"]])

# Mean expense per person with an expense, overall and by age group ------------------
# (DOMAIN X_ANYSVCE('1') X_ANYSVCE('1')*AGECAT)
d1 = surveymeans(PUF192, "TOTAL", weight="PERWT16F", strata="VARSTR", cluster="VARPSU", domain="X_ANYSVCE")
d2 = surveymeans(PUF192, "TOTAL", weight="PERWT16F", strata="VARSTR", cluster="VARPSU", domain=["X_ANYSVCE", "AGECAT"])
dom = pd.concat([d1[d1.X_ANYSVCE == 1], d2[d2.X_ANYSVCE == 1]], ignore_index=True)
dom["AGECAT"] = sas_format(dom.AGECAT, AGECAT, missing="ALL AGES")
show("Mean expense per person with an expense", dom[["AGECAT", "N", "SumWgt", "Mean", "StdErr", "Sum", "StdDev"]])
