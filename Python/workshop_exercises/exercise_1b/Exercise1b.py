"""Python port of SAS/workshop_exercises/exercise_1b/Exercise1b.sas

DESCRIPTION:  THIS PROGRAM GENERATES THE FOLLOWING ESTIMATES ON NATIONAL HEALTH CARE EXPENSES BY TYPE OF SERVICE, 2015:

               (1) PERCENTAGE DISTRIBUTION OF EXPENSES BY TYPE OF SERVICE
               (2) PERCENTAGE OF PERSONS WITH AN EXPENSE, BY TYPE OF SERVICE
               (3) MEAN EXPENSE PER PERSON WITH AN EXPENSE, BY TYPE OF SERVICE

              DEFINED SERVICE CATEGORIES ARE:
                 HOSPITAL INPATIENT
                 AMBULATORY SERVICE: OFFICE-BASED & HOSPITAL OUTPATIENT VISITS
                 PRESCRIBED MEDICINES
                 DENTAL VISITS
                 EMERGENCY ROOM
                 HOME HEALTH CARE (AGENCY & NON-AGENCY) AND OTHER (TOTAL EXPENDITURES - ABOVE EXPENDITURE CATEGORIES)

             NOTE: EXPENSES INCLUDE BOTH FACILITY AND PHYSICIAN EXPENSES.

 INPUT FILE:   <MEPS_DIR>/SAS/DATA/H181.SAS7BDAT (2015 FULL-YEAR FILE)

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
FLAG = {0: "No expense", 1: "Any expense"}
GTZERO = {0: "$0", slice(1e-12, None): ">$0"}

# Load FYC file and define variables (SAS: DATA PUF181; SET CDATA.H181 (KEEP=...)) --
PUF181 = read_meps("h181", ["TOTEXP15", "IPDEXP15", "IPFEXP15", "OBVEXP15", "RXEXP15", "OPDEXP15", "OPFEXP15",
                            "DVTEXP15", "ERDEXP15", "ERFEXP15", "HHAEXP15", "HHNEXP15", "OTHEXP15", "VISEXP15",
                            "AGE15X", "AGE42X", "AGE31X", "VARSTR", "VARPSU", "PERWT15F"])
P = PUF181
P["TOTAL"] = P.TOTEXP15
P["HOSPITAL_INPATIENT"] = P.IPDEXP15 + P.IPFEXP15
P["AMBULATORY"] = P.OBVEXP15 + P.OPDEXP15 + P.OPFEXP15 + P.ERDEXP15 + P.ERFEXP15
P["PRESCRIBED_MEDICINES"] = P.RXEXP15
P["DENTAL"] = P.DVTEXP15
P["HOME_HEALTH_OTHER"] = P.HHAEXP15 + P.HHNEXP15 + P.OTHEXP15 + P.VISEXP15
P["DIFF"] = (P.TOTAL - P.HOSPITAL_INPATIENT - P.AMBULATORY - P.PRESCRIBED_MEDICINES - P.DENTAL
             - P.HOME_HEALTH_OTHER)

# SAS ARRAY loop: X_<category> = 1 if any expense in that category
EXX = ["TOTAL", "HOSPITAL_INPATIENT", "AMBULATORY", "PRESCRIBED_MEDICINES", "DENTAL", "HOME_HEALTH_OTHER"]
ANYX = ["X_ANYSVCE", "X_HOSPITAL_INPATIENT", "X_AMBULATORY", "X_PRESCRIBED_MEDICINES", "X_DENTAL",
        "X_HOME_HEALTH_OTHER"]
for e, x in zip(EXX, ANYX):
    P[x] = np.where(P[e] > 0, 1, 0)

P["AGE"] = np.select([P.AGE15X >= 0, P.AGE42X >= 0, P.AGE31X >= 0], [P.AGE15X, P.AGE42X, P.AGE31X], np.nan)
P["AGECAT"] = np.select([P.AGE.between(0, 64), P.AGE > 64], [1, 2], np.nan)

# QC (PROC FREQ ... / LIST MISSING)
for e, x in zip(EXX, ANYX):
    print(freq_list(P.assign(**{e: sas_format(P[e], GTZERO), x: sas_format(P[x], FLAG)}), [x, e]))
print(freq_list(P, ["AGECAT"]))
print(freq_list(P, ["DIFF"]))

# Total expenses by type of service, and percentage distribution (RATIO) -----------
show("Total expenses by type of service",
     surveymeans(P, EXX[1:] + ["TOTAL"], weight="PERWT15F", strata="VARSTR", cluster="VARPSU")
     [["VarName", "N", "SumWgt", "Mean", "StdErr", "Sum", "StdDev"]])
ratios = pd.concat([surveyratio(P, e, "TOTAL", weight="PERWT15F", strata="VARSTR", cluster="VARPSU")
                    for e in EXX[1:]])
show("Ratio analysis: share of total expenses by type of service", ratios)

# Percentage of persons with an expense, by type of service --------------------------
show("Percentage of persons with an expense, by type of service",
     surveymeans(P, ANYX, weight="PERWT15F", strata="VARSTR", cluster="VARPSU")[["VarName", "N", "Mean", "StdErr", "Sum"]])

# Mean expense per person: overall, by age group, and among persons with an expense --
# (DOMAIN AGECAT AGECAT*X_ANYSVCE('1'))
overall = surveymeans(P, "TOTAL", weight="PERWT15F", strata="VARSTR", cluster="VARPSU")
d1 = surveymeans(P, "TOTAL", weight="PERWT15F", strata="VARSTR", cluster="VARPSU", domain="AGECAT")
d2 = surveymeans(P, "TOTAL", weight="PERWT15F", strata="VARSTR", cluster="VARPSU", domain=["AGECAT", "X_ANYSVCE"])
combine = pd.concat([overall, d1, d2[d2.X_ANYSVCE == 1]], ignore_index=True)
combine["AGECAT"] = sas_format(combine.AGECAT, AGECAT, missing="All Ages")
combine["X_ANYSVCE"] = sas_format(combine.X_ANYSVCE, FLAG, missing="No or any expense")
show("Mean and total expense per person", combine[["AGECAT", "X_ANYSVCE", "N", "SumWgt", "Mean", "StdErr", "Sum", "StdDev"]])

# Mean expense per person with an expense, by type of service (overall and by age) ---
for e, x in zip(EXX[1:], ANYX[1:]):
    a = surveymeans(P, e, weight="PERWT15F", strata="VARSTR", cluster="VARPSU", domain=x)
    b = surveymeans(P, e, weight="PERWT15F", strata="VARSTR", cluster="VARPSU", domain=["AGECAT", x])
    t = pd.concat([a[a[x] == 1], b[b[x] == 1]], ignore_index=True)
    t["AGECAT"] = sas_format(t.AGECAT, AGECAT, missing="All Ages")
    show(f"{e}: mean expense per person with an expense ({x} = 1)",
         t[["AGECAT", "N", "SumWgt", "Mean", "StdErr", "Sum", "StdDev"]])
