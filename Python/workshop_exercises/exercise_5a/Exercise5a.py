# -----------------------------------------------------------------------------
# DESCRIPTION:  THIS PROGRAM ILLUSTRATES HOW TO CONSTRUCT FAMILY-LEVEL VARIABLES
#               FROM PERSON-LEVEL DATA
#
#               THERE ARE TWO DEFINITIONS OF FAMILY UNIT IN MEPS.
#                  1) CPS FAMILY:  ID IS DUID + CPSFAMID.  CORRESPONDING WEIGHT IS FAMWT15C.
#                  2) MEPS FAMILY: ID IS DUID + FAMIDYR.   CORRESPONDING WEIGHT IS FAMWT15F.
#
#               THE CPS FAMILY IS USED IN THIS EXERCISE.
#
# INPUT FILE:   C:\MEPS\SAS\DATA\H181.SAS7BDAT (2015 FY PUF DATA)
#
# Python port of SAS/workshop_exercises/exercise_5a/Exercise5a.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import load_meps, svy_mean  # noqa: E402

pd.set_option("display.width", 200)

print("2018 AHRQ MEPS DATA USERS WORKSHOP")
print("EXERCISE3: CALCULATE FAMILY-LEVEL ESTIMATES\n")

H181 = load_meps(
    "h181",
    columns=["DUPERSID", "DUID", "PID", "AGE15X", "SEX", "CPSFAMID", "FAMWT15C", "FAMIDYR", "FAMWT15F",
             "VARSTR", "VARPSU", "TOTSLF15", "TTLP15X"],
)  # fmt: skip

print("SAMPLE DUMP FOR FAMILY IDS")
print(H181[["DUID", "PID", "AGE15X", "SEX", "CPSFAMID", "FAMWT15C", "FAMIDYR", "FAMWT15F"]].head(20).to_string(index=False))

PERS = H181[["DUPERSID", "DUID", "CPSFAMID", "FAMWT15C", "VARSTR", "VARPSU", "TOTSLF15", "TTLP15X"]].sort_values(
    ["DUID", "CPSFAMID"], kind="stable"
)

# Running family totals (FIRST./LAST. BY-group processing) --------------------
#  FAMSIZE = # OF PERSONS PER CPS FAMILY
#  FAMOOP  = TOTAL OUT-OF-POCKET EXP (TOTSLF15) PER CPS FAMILY
#  FAMINC  = TOTAL INCOME (TTLP15X) PER CPS FAMILY
g = PERS.groupby(["DUID", "CPSFAMID"], sort=False)
PERS2 = PERS.assign(FAMSIZE=g.cumcount() + 1, FAMOOP=g["TOTSLF15"].cumsum(), FAMINC=g["TTLP15X"].cumsum())
FAM = (
    PERS.groupby(["DUID", "CPSFAMID"])
    .agg(FAMSIZE=("DUPERSID", "size"), FAMOOP=("TOTSLF15", "sum"), FAMINC=("TTLP15X", "sum"))
    .reset_index()
)

print("\nA SAMPLE DUMP TO CHECK THE CREATION OF THE FAMILY-LEVEL VARIABLES")
print(PERS2.head(20).to_string(index=False))

# ADD WEIGHT, VARSTR, AND VARPSU TO THE FAMILY-LEVEL ANALYTIC DATA ------------
FAMWT = PERS[PERS["FAMWT15C"] > 0].drop_duplicates(["DUID", "CPSFAMID"])[
    ["DUID", "CPSFAMID", "FAMWT15C", "VARSTR", "VARPSU"]
]
FAM2 = FAM.merge(FAMWT, on=["DUID", "CPSFAMID"], how="inner")

print("\nCPS FAMILY-LEVEL ESTIMATES ON FAMILY SIZE, OUT-OF-POCKET EXP, AND INCOME, 2015")
res = svy_mean(FAM2, ["FAMSIZE", "FAMOOP", "FAMINC"], weight="FAMWT15C", strata="VARSTR", psu="VARPSU")
res["SumWgt"] = FAM2["FAMWT15C"].sum()
print(res.to_string(index=False))
