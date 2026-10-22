# -----------------------------------------------------------------------------
# DESCRIPTION:  THIS PROGRAM GENERATES THE FOLLOWING ESTIMATES ON NATIONAL
#               HEALTH CARE EXPENSES, 2016:
#                (1) OVERALL EXPENSES
#                (2) PERCENTAGE OF PERSONS WITH AN EXPENSE
#                (3) MEAN EXPENSE PER PERSON WITH AN EXPENSE
#
# INPUT FILE:   C:\MEPS\SAS\DATA\H192.SAS7BDAT (2016 FULL-YEAR FILE)
#
# Python port of SAS/workshop_exercises/exercise_1a/Exercise1a.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, crosstab_list, load_meps, svy_mean, svy_total  # noqa: E402

pd.set_option("display.width", 200)

# Formats (PROC FORMAT) -------------------------------------------------------

AGEF = [(0, 64, "0-64"), (65, np.inf, "65+")]
AGECAT = {1: "0-64", 2: "65+"}
GTZERO = [(0, 0, "0"), (1e-12, np.inf, ">0")]

print("2018 AHRQ MEPS DATA USERS WORKSHOP")
print("EXERCISE1: NATIONAL HEALTH CARE EXPENSES, 2016\n")

# READ IN DATA FROM 2016 CONSOLIDATED DATA FILE (HC-192) ----------------------

PUF192 = load_meps(
    "h192", columns=["TOTEXP16", "AGE16X", "AGE42X", "AGE31X", "VARSTR", "VARPSU", "PERWT16F"]
)
PUF192["TOTAL"] = PUF192["TOTEXP16"]

# CREATE FLAG (1/0) VARIABLES FOR PERSONS WITH AN EXPENSE
PUF192["X_ANYSVCE"] = (PUF192["TOTAL"] > 0).astype(int)

# CREATE A SUMMARY VARIABLE FROM END OF YEAR, 42, AND 31 VARIABLES
PUF192["AGE"] = np.select(
    [PUF192["AGE16X"] >= 0, PUF192["AGE42X"] >= 0, PUF192["AGE31X"] >= 0],
    [PUF192["AGE16X"], PUF192["AGE42X"], PUF192["AGE31X"]],
    default=np.nan,
)
PUF192["AGECAT"] = np.select([PUF192["AGE"].between(0, 64), PUF192["AGE"] > 64], [1, 2], default=np.nan)

# Supporting crosstabs for the flag variables (PROC FREQ / LIST MISSING) -------

print("Supporting crosstabs for the flag variables")
print(crosstab_list(PUF192.assign(TOTAL=apply_format(PUF192["TOTAL"], GTZERO)), ["X_ANYSVCE", "TOTAL"]))
print(crosstab_list(PUF192.assign(AGE=apply_format(PUF192["AGE"], AGEF)), ["AGECAT", "AGE"]), "\n")

# PERCENTAGE OF PERSONS WITH AN EXPENSE & OVERALL EXPENSES --------------------

design = dict(weight="PERWT16F", strata="VARSTR", psu="VARPSU")
means = svy_mean(PUF192, ["X_ANYSVCE", "TOTAL"], **design)
sums = svy_total(PUF192, ["X_ANYSVCE", "TOTAL"], **design)
overall = means.merge(sums, on=["variable", "n"], suffixes=("_mean", "_sum"))
overall["SumWgt"] = PUF192.loc[PUF192["PERWT16F"] > 0, "PERWT16F"].sum()

print("PERCENTAGE OF PERSONS WITH AN EXPENSE")
print(overall[overall["variable"] == "X_ANYSVCE"].to_string(index=False))
print("\nOVERALL EXPENSES")
print(overall[overall["variable"] == "TOTAL"].to_string(index=False))

# MEAN EXPENSE PER PERSON WITH AN EXPENSE, FOR OVERALL, AGE 0-64, AND AGE 65+ --
#  DOMAIN X_ANYSVCE('1')  X_ANYSVCE('1')*AGECAT

PUF192["AGECAT"] = apply_format(PUF192["AGECAT"], AGECAT)
rows = []
for dom in (["X_ANYSVCE"], ["X_ANYSVCE", "AGECAT"]):
    m = svy_mean(PUF192, "TOTAL", domain=dom, **design)
    s = svy_total(PUF192, "TOTAL", domain=dom, **design)
    res = m.merge(s, on=dom + ["variable", "n"], suffixes=("_mean", "_sum"))
    res = res[res["X_ANYSVCE"] == 1]
    if "AGECAT" not in res:
        res["AGECAT"] = "ALL AGES"
    rows.append(res)
domain_results = pd.concat(rows, ignore_index=True)
domain_results = domain_results[domain_results["AGECAT"] != "Missing"]

print("\nMEAN EXPENSE PER PERSON WITH AN EXPENSE, FOR OVERALL, AGE 0-64, AND AGE 65+")
print(domain_results.drop(columns=["X_ANYSVCE", "variable"]).to_string(index=False))
