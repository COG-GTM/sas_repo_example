# -----------------------------------------------------------------------------
# This program generates National Totals and Per-person Averages for Narcotic
#  analgesics and Narcotic analgesic combos care for the U.S. civilian
#  non-institutionalized population (2018), including:
#   - Number of purchases (fills)
#   - Total expenditures
#   - Out-of-pocket payments
#   - Third-party payments
#
#  Input files:
#     - 2018 Prescribed medicines file (h206a)
#     - 2018 Full-year consolidated file (h209)
#
# Python port of SAS/workshop_exercises/exercise_2c/Exercise2c.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, load_meps, svy_mean, svy_total  # noqa: E402

pd.set_option("display.width", 200)

# Formats (PROC FORMAT) -------------------------------------------------------

SUBPOP = {1: "OnePlusNacroticEtc", 2: "OTHERS"}

# KEEP THE SPECIFIED VARIABLES WHEN READING THE INPUT DATA SET AND RESTRICT TO
# OBSERVATIONS HAVING THERAPEUTIC CLASSIFICATION (TC) CODES FOR NARCOTIC
# ANALGESICS OR NARCOTIC ANALGESIC COMBOS

H206A = load_meps("h206a", columns=["DUPERSID", "RXRECIDX", "LINKIDX", "TC1S1_1", "RXXP18X", "RXSF18X"])
DRUG = H206A[H206A["TC1S1_1"].isin([60, 191])]

print("A SAMPLE DUMP FOR PMED RECORDS WITH Narcotic analgesics or Narcotic analgesic combos, 2018")
print(DRUG.head(12).to_string(index=False))

# SUM "RXXP18X and RXSF18X" DATA TO PERSON-LEVEL -----------------------------

PERDRUG2 = (
    DRUG.groupby("DUPERSID")
    .agg(N_PHRCHASE=("RXXP18X", "size"), TOT=("RXXP18X", "sum"), OOP=("RXSF18X", "sum"))
    .reset_index()
)
# CREATE A NEW VARIABLE FOR EXPENSES EXCLUDING OUT-OF-POCKET EXPENSES
PERDRUG2["THIRD_PAYER"] = PERDRUG2["TOT"] - PERDRUG2["OOP"]

print("\nA SAMPLE DUMP FOR PERSON-LEVEL EXPENDITURES FOR NARCOTIC ANALGESICS OR NARCOTIC ANALGESIC COMBOS")
print(PERDRUG2.head(3).to_string(index=False))
print("Total purchases (sum of _FREQ_):", PERDRUG2["N_PHRCHASE"].sum())

# MERGE THE PERSON-LEVEL EXPENDITURES TO THE FY PUF ---------------------------

H209 = load_meps("h209", columns=["DUPERSID", "VARSTR", "VARPSU", "PERWT18F"])
FY = H209.merge(PERDRUG2, on="DUPERSID", how="left", indicator=True)
# 1: PERSONS WITH 1+ Narcotic analgesics or combos; 2: PERSONS WITHOUT ANY PURCHASE
FY["SUBPOP"] = np.where(FY.pop("_merge") == "both", 1, 2)
FY[["N_PHRCHASE", "TOT", "OOP", "THIRD_PAYER"]] = FY[["N_PHRCHASE", "TOT", "OOP", "THIRD_PAYER"]].fillna(0)
FY["SUBPOP"] = apply_format(FY["SUBPOP"], SUBPOP)

# CALCULATE ESTIMATES ON USE AND EXPENDITURES ---------------------------------
#  DOMAIN SUBPOP("OnePlusNacroticEtc")

print("\nPERSON-LEVEL ESTIMATES ON EXPENDITURES AND USE FOR NARCOTIC ANALGESICS or NARCOTIC COMBOS, 2018")
design = dict(weight="PERWT18F", strata="VARSTR", psu="VARPSU", domain="SUBPOP")
vars_ = ["N_PHRCHASE", "TOT", "OOP", "THIRD_PAYER"]
res = svy_mean(FY, vars_, **design).merge(
    svy_total(FY, vars_, **design), on=["SUBPOP", "variable", "n"], suffixes=("_mean", "_sum")
)
print(res[res["SUBPOP"] == "OnePlusNacroticEtc"].to_string(index=False))
