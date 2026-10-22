# -----------------------------------------------------------------------------
# PURPOSE:  THIS PROGRAM GENERATES SELECTED ESTIMATES FOR A 2015 VERSION OF THE
#           MEPS STATISTICS BRIEF # 275: "Trends in Antipsychotics Purchases and
#           Expenses for the U.S. Civilian Noninstitutionalized Population,
#           1997 and 2007"
#
#   (1) FIGURE 1: TOTAL EXPENSE FOR ANTIPSYCHOTICS
#   (2) FIGURE 2: TOTAL NUMBER OF PURCHASES OF ANTIPSYCHOTICS
#   (3) FIGURE 3: TOTAL NUMBER OF PERSONS PURCHASING ONE OR MORE ANTIPSYCHOTICS
#   (4) FIGURE 4: AVERAGE TOTAL, OUT OF POCKET, AND THIRD PARTY PAYER EXPENSE
#                 FOR ANTIPSYCHOTICS PER PERSON WITH AN ANTIPSYCHOTIC MEDICINE
#                 PURCHASE
#
# INPUT FILES:  (1) C:\MEPS\SAS\DATA\H181.SAS7BDAT (2015 FULL-YEAR CONSOLIDATED PUF)
#               (2) C:\MEPS\SAS\DATA\H178A.SAS7BDAT (2015 PRESCRIBED MEDICINES PUF)
#
# Python port of SAS/workshop_exercises/exercise_2a/Exercise2a.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, crosstab_list, load_meps, svy_mean, svy_total  # noqa: E402

pd.set_option("display.width", 200)

print("2018 AHRQ MEPS DATA USERS WORKSHOP")
print("EXERCISE2: Antipsychotics Purchases and Expenses, 2015\n")

GTZERO = [(0, 0, "0"), (1e-12, np.inf, ">0")]

# 1) IDENTIFY ANTIPSYCHOTIC DRUGS USING THERAPEUTIC CLASSIFICATION (TC) CODES --

H178A = load_meps("h178a")
DRUG = H178A[(H178A["TC1"] == 242) & (H178A["TC1S1"] == 251)]  # definition of ANTIPSYCHOTIC DRUGS

print("A SAMPLE DUMP FOR PMED RECORDS WITH ANTIPSYCHOTIC DRUGS")
print(DRUG[["DUPERSID", "RXRECIDX", "LINKIDX", "TC1", "TC1S1", "RXXP15X", "RXSF15X"]].head(30).to_string(index=False))

# 2) SUM DATA TO PERSON-LEVEL (PROC SUMMARY NWAY; CLASS DUPERSID) ------------

PERDRUG2 = (
    DRUG.groupby("DUPERSID")
    .agg(N_PHRCHASE=("RXXP15X", "size"), TOT=("RXXP15X", "sum"), OOP=("RXSF15X", "sum"))
    .reset_index()
)
PERDRUG2["THIRD_PAYER"] = PERDRUG2["TOT"] - PERDRUG2["OOP"]
print("\nA SAMPLE DUMP FOR PERSON-LEVEL EXPENDITURES FOR ANTIPSYCHOTIC DRUGS")
print(PERDRUG2.head(30).to_string(index=False))

# 3) MERGE THE PERSON-LEVEL EXPENDITURES TO THE FY PUF -----------------------

H181 = load_meps("h181", columns=["DUPERSID", "VARSTR", "VARPSU", "PERWT15F"])
FY = H181.merge(PERDRUG2, on="DUPERSID", how="left", indicator=True)
# SUB = 1: persons with 1+ antipsychotic purchase; 2: without any purchase
FY["SUB"] = np.where(FY.pop("_merge") == "both", 1, 2)
FY[["N_PHRCHASE", "TOT", "OOP", "THIRD_PAYER"]] = FY[["N_PHRCHASE", "TOT", "OOP", "THIRD_PAYER"]].fillna(0)

print("\nSUPPORTING CROSSTABS FOR NEW VARIABLES")
qc = FY.copy()
for v in ("N_PHRCHASE", "TOT", "OOP", "THIRD_PAYER"):
    qc[v] = apply_format(qc[v], GTZERO)
print(crosstab_list(qc, ["SUB", "N_PHRCHASE", "TOT", "OOP", "THIRD_PAYER"]))

# 4) CALCULATE ESTIMATES ON EXPENDITURES AND USE (DOMAIN SUB('1')) ------------

print("\nPERSON-LEVEL ESTIMATES ON EXPENDITURES AND USE FOR ANTIPSYCHOTIC DRUGS, 2015")
design = dict(weight="PERWT15F", strata="VARSTR", psu="VARPSU", domain="SUB")
vars_ = ["TOT", "N_PHRCHASE", "OOP", "THIRD_PAYER"]
res = svy_total(FY, vars_, **design).merge(
    svy_mean(FY, vars_, **design), on=["SUB", "variable", "n"], suffixes=("_sum", "_mean")
)
print(res[res["SUB"] == 1].to_string(index=False))
