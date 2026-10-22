# -----------------------------------------------------------------------------
# PURPOSE:  THIS PROGRAM GENERATES SELECTED ESTIMATES FOR A 2016 VERSION OF THE
#           Purchases and Expenses for Narcotic analgesics or Narcotic analgesic
#           combos
#
#   (1) FIGURE 1: TOTAL EXPENSE FOR Narcotic analgesics or Narcotic analgesic combos
#   (2) FIGURE 2: TOTAL NUMBER OF PURCHASES OF Narcotic analgesics or Narcotic
#                 analgesic combos
#   (3) FIGURE 3: TOTAL NUMBER OF PERSONS PURCHASING ONE OR MORE Narcotic
#                 analgesics or Narcotic analgesic combos
#   (4) FIGURE 4: AVERAGE TOTAL, OUT OF POCKET, AND THIRD PARTY PAYER EXPENSE
#                 FOR Narcotic analgesics or Narcotic analgesic combos PER PERSON
#                 WITH AN Narcotic analgesics or Narcotic analgesic combos
#                 MEDICINE PURCHASE
#
# INPUT FILES:  (1) C:\MEPS\SAS\DATA\H192.SAS7BDAT (2016 FULL-YEAR CONSOLIDATED PUF)
#               (2) C:\MEPS\SAS\DATA\H188A.SAS7BDAT (2016 PRESCRIBED MEDICINES PUF)
#
# Python port of SAS/workshop_exercises/exercise_2b/Exercise2b.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, crosstab_list, load_meps, svy_mean, svy_total  # noqa: E402

pd.set_option("display.width", 200)

print("2018 AHRQ MEPS DATA USERS WORKSHOP")
print("EXERCISE2: Narcotic analgesics or Narcotic analgesic combos, 2016\n")

GTZERO = [(0, 0, "0"), (1e-12, np.inf, ">0")]

# 1) IDENTIFY Narcotic analgesics or Narcotic analgesic combos USING
#    THERAPEUTIC CLASSIFICATION (TC) CODES ------------------------------------

H188A = load_meps("h188a")
DRUG = H188A[H188A["TC1S1_1"].isin([60, 191])]  # definition of Narcotic analgesics or combos

print("A SAMPLE DUMP FOR PMED RECORDS WITH Narcotic analgesics or Narcotic analgesic combos")
print(DRUG[["DUPERSID", "RXRECIDX", "LINKIDX", "TC1S1_1", "RXXP16X", "RXSF16X"]].head(30).to_string(index=False))

# 2) SUM DATA TO PERSON-LEVEL (PROC SUMMARY NWAY; CLASS DUPERSID) ------------

PERDRUG2 = (
    DRUG.groupby("DUPERSID")
    .agg(N_PHRCHASE=("RXXP16X", "size"), TOT=("RXXP16X", "sum"), OOP=("RXSF16X", "sum"))
    .reset_index()
)
PERDRUG2["THIRD_PAYER"] = PERDRUG2["TOT"] - PERDRUG2["OOP"]
print("\nA SAMPLE DUMP FOR PERSON-LEVEL EXPENDITURES FOR Narcotic analgesics or Narcotic analgesic combos")
print(PERDRUG2.head(30).to_string(index=False))

# 3) MERGE THE PERSON-LEVEL EXPENDITURES TO THE FY PUF -----------------------

H192 = load_meps("h192", columns=["DUPERSID", "VARSTR", "VARPSU", "PERWT16F"])
FY = H192.merge(PERDRUG2, on="DUPERSID", how="left", indicator=True)
# SUB = 1: persons with 1+ purchase; 2: persons without any purchase
FY["SUB"] = np.where(FY.pop("_merge") == "both", 1, 2)
FY[["N_PHRCHASE", "TOT", "OOP", "THIRD_PAYER"]] = FY[["N_PHRCHASE", "TOT", "OOP", "THIRD_PAYER"]].fillna(0)

print("\nSUPPORTING CROSSTABS FOR NEW VARIABLES")
qc = FY.copy()
for v in ("N_PHRCHASE", "TOT", "OOP", "THIRD_PAYER"):
    qc[v] = apply_format(qc[v], GTZERO)
print(crosstab_list(qc, ["SUB", "N_PHRCHASE", "TOT", "OOP", "THIRD_PAYER"]))

# 4) CALCULATE ESTIMATES ON USE AND EXPENDITURES (DOMAIN SUB('1')) ------------

print("\nPERSON-LEVEL ESTIMATES ON EXPENDITURES AND USE FOR Narcotic analgesics or Narcotic analgesic combos, 2016")
print("SUBSET THE ESTIMATES FOR PERSONS ONLY WITH 1+ Narcotic analgesics or Narcotic analgesic combos")
design = dict(weight="PERWT16F", strata="VARSTR", psu="VARPSU", domain="SUB")
vars_ = ["TOT", "N_PHRCHASE", "OOP", "THIRD_PAYER"]
res = svy_total(FY, vars_, **design).merge(
    svy_mean(FY, vars_, **design), on=["SUB", "variable", "n"], suffixes=("_sum", "_mean")
)
print(res[res["SUB"] == 1].to_string(index=False))
