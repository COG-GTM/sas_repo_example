# -----------------------------------------------------------------------------
# DESCRIPTION:  THIS PROGRAM GENERATES THE FOLLOWING ESTIMATES ON NATIONAL
#               HEALTH CARE EXPENSES BY TYPE OF SERVICE, 2015:
#                (1) PERCENTAGE DISTRIBUTION OF EXPENSES BY TYPE OF SERVICE
#                (2) PERCENTAGE OF PERSONS WITH AN EXPENSE, BY TYPE OF SERVICE
#                (3) MEAN EXPENSE PER PERSON WITH AN EXPENSE, BY TYPE OF SERVICE
#
#               DEFINED SERVICE CATEGORIES ARE:
#                  HOSPITAL INPATIENT
#                  AMBULATORY SERVICE: OFFICE-BASED & HOSPITAL OUTPATIENT VISITS
#                  PRESCRIBED MEDICINES
#                  DENTAL VISITS
#                  EMERGENCY ROOM
#                  HOME HEALTH CARE (AGENCY & NON-AGENCY) AND OTHER (TOTAL
#                  EXPENDITURES - ABOVE EXPENDITURE CATEGORIES)
#
#              NOTE: EXPENSES INCLUDE BOTH FACILITY AND PHYSICIAN EXPENSES.
#
# INPUT FILE:   C:\MEPS\SAS\DATA\H181.SAS7BDAT (2015 FULL-YEAR FILE)
#
# Python port of SAS/workshop_exercises/exercise_1b/Exercise1b.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, crosstab_list, load_meps, svy_mean, svy_ratio, svy_total  # noqa: E402

pd.set_option("display.width", 200)

# Formats (PROC FORMAT) -------------------------------------------------------

AGEF = [(0, 64, "0-64"), (65, np.inf, "65+")]
AGECAT = {1: "0-64", 2: "65+"}
GTZERO = [(0, 0, "$0"), (1e-12, np.inf, ">$0")]
FLAG = {0: "No expense", 1: "Any expense"}

print("2018 AHRQ MEPS DATA USERS WORKSHOP")
print("EXERCISE1: NATIONAL HEALTH CARE EXPENSES, 2015\n")

# READ IN DATA FROM 2015 CONSOLIDATED DATA FILE (HC-181) ----------------------

keep = [
    "TOTEXP15", "IPDEXP15", "IPFEXP15", "OBVEXP15", "RXEXP15", "OPDEXP15", "OPFEXP15", "DVTEXP15",
    "ERDEXP15", "ERFEXP15", "HHAEXP15", "HHNEXP15", "OTHEXP15", "VISEXP15", "AGE15X", "AGE42X",
    "AGE31X", "VARSTR", "VARPSU", "PERWT15F",
]  # fmt: skip
PUF181 = load_meps("h181", columns=keep)

# Define expenditure variables by type of service
P = PUF181
P["TOTAL"] = P["TOTEXP15"]
P["HOSPITAL_INPATIENT"] = P["IPDEXP15"] + P["IPFEXP15"]
P["AMBULATORY"] = P["OBVEXP15"] + P["OPDEXP15"] + P["OPFEXP15"] + P["ERDEXP15"] + P["ERFEXP15"]
P["PRESCRIBED_MEDICINES"] = P["RXEXP15"]
P["DENTAL"] = P["DVTEXP15"]
P["HOME_HEALTH_OTHER"] = P["HHAEXP15"] + P["HHNEXP15"] + P["OTHEXP15"] + P["VISEXP15"]

# QC CHECK IF THE SUM OF EXPENDITURES BY TYPE OF SERVICE IS EQUAL TO TOTAL
types = ["HOSPITAL_INPATIENT", "AMBULATORY", "PRESCRIBED_MEDICINES", "DENTAL", "HOME_HEALTH_OTHER"]
P["DIFF"] = P["TOTAL"] - P[types].sum(axis=1)

# CREATE FLAG (1/0) VARIABLES FOR PERSONS WITH AN EXPENSE, BY TYPE OF SERVICE
EXX = ["TOTAL"] + types
ANYX = ["X_ANYSVCE"] + [f"X_{t}" for t in types]
for e, x in zip(EXX, ANYX):
    P[x] = (P[e] > 0).astype(int)

# CREATE A SUMMARY VARIABLE FROM END OF YEAR, 42, AND 31 VARIABLES
P["AGE"] = np.select(
    [P["AGE15X"] >= 0, P["AGE42X"] >= 0, P["AGE31X"] >= 0],
    [P["AGE15X"], P["AGE42X"], P["AGE31X"]],
    default=np.nan,
)
P["AGECAT"] = np.select([P["AGE"].between(0, 64), P["AGE"] > 64], [1, 2], default=np.nan)

# Supporting crosstabs for the flag variables (PROC FREQ / LIST MISSING) -------

print("Supporting crosstabs for the flag variables")
qc = P.copy()
for e, x in zip(EXX, ANYX):
    qc[e] = apply_format(qc[e], GTZERO)
    qc[x] = apply_format(qc[x], FLAG)
    print(crosstab_list(qc, [x, e]))
qc["AGE"] = apply_format(qc["AGE"], AGEF)
qc["AGECAT"] = apply_format(qc["AGECAT"], AGECAT, missing="All Ages")
print(crosstab_list(qc, ["AGECAT", "AGE"]))
print(crosstab_list(qc.assign(DIFF=qc["DIFF"].round(6)), ["DIFF"]), "\n")

design = dict(weight="PERWT15F", strata="VARSTR", psu="VARPSU")

# PERCENTAGE DISTRIBUTION OF EXPENSES BY TYPE OF SERVICE (STAT BRIEF #491 FIG 1)
print("PERCENTAGE DISTRIBUTION OF EXPENSES BY TYPE OF SERVICE (STAT BRIEF #491 FIGURE 1)")
print(svy_total(P, types + ["TOTAL"], **design).to_string(index=False))
print(svy_ratio(P, types, "TOTAL", **design).to_string(index=False), "\n")

# PERCENTAGE OF PERSONS WITH AN EXPENSE, BY TYPE OF SERVICE
print("PERCENTAGE OF PERSONS WITH AN EXPENSE, BY TYPE OF SERVICE")
pct = svy_mean(P, ANYX, **design).merge(
    svy_total(P, ANYX, **design), on=["variable", "n"], suffixes=("_mean", "_sum")
)
print(pct.to_string(index=False), "\n")

# MEAN TOTAL EXPENSE PER PERSON WITH AN EXPENSE, AGE 0-64, AND AGE 65+ ---------
#  DOMAIN AGECAT AGECAT*X_ANYSVCE('1')

P["AGECAT"] = apply_format(P["AGECAT"], AGECAT)


def mean_sum(var, domain=None):
    m = svy_mean(P, var, domain=domain, **design)
    s = svy_total(P, var, domain=domain, **design)
    keys = (domain or []) + ["variable", "n"]
    return m.merge(s, on=keys, suffixes=("_mean", "_sum"))


combine = pd.concat(
    [
        mean_sum("TOTAL").assign(AGECAT="All Ages"),
        mean_sum("TOTAL", ["AGECAT"]),
        mean_sum("TOTAL", ["AGECAT", "X_ANYSVCE"]).query("X_ANYSVCE == 1"),
    ],
    ignore_index=True,
)
combine["X_ANYSVCE"] = apply_format(combine["X_ANYSVCE"], FLAG, missing="No or any expense")
print("MEAN TOTAL EXPENSE PER PERSON WITH AN EXPENSE, AGE 0-64, AND AGE 65+")
cols = ["AGECAT", "X_ANYSVCE", "n", "estimate_mean", "se_mean", "estimate_sum", "se_sum"]
print(combine[cols].to_string(index=False))

# MEAN EXPENSE PER PERSON WITH AN EXPENSE, BY TYPE OF SERVICE, AGE 0-64, 65+ ---
#  DOMAIN X_<type>('1') AGECAT*X_<type>('1')
titles = {
    "HOSPITAL_INPATIENT": "MEAN HOSPITAL INPATIENT EXPENSE PER PERSON WITH AN INPATIENT EXPENSE",
    "AMBULATORY": "MEAN AMBULATORY EXPENSE PER PERSON WITH AN AMBULATORY EXPENSE",
    "PRESCRIBED_MEDICINES": "MEAN PRESCRIPTION MEDICINE EXPENSE PER PERSON WITH A PRESCRIPTION MEDICINE EXPENSE",
    "DENTAL": "MEAN DENTAL EXPENSE PER PERSON WITH A DENTAL EXPENSE",
    "HOME_HEALTH_OTHER": "MEAN OTHER EXPENSE (INCLUDING HOME HEALTH EXPENSE) PER PERSON WITH AN OTHER EXPENSE",
}
for t, title in titles.items():
    flag = f"X_{t}"
    res = pd.concat(
        [
            mean_sum(t, [flag]).assign(AGECAT="All Ages"),
            mean_sum(t, ["AGECAT", flag]),
        ],
        ignore_index=True,
    )
    res = res[(res[flag] == 1) & (res["AGECAT"] != "Missing")]
    print(f"\n{title}, AGE 0-64, AND AGE 65+")
    print(res[["AGECAT", "n", "estimate_mean", "se_mean", "estimate_sum", "se_sum"]].to_string(index=False))
