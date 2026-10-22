# ----------------------------------------------------------------
# PROGRAM:      C:\MEPS\PROG\E6.SAS
#
# DESCRIPTION:  THIS PROGRAM GENERATES THE FOLLOWING ESTIMATES FOR 2005
#               PERCENTAGE DISTRIBUTION OF HEALTH CARE SPENDING, BY TYPE OF SERVICE,
#               FOR THE U.S. CIVILIAN NONINSTITUTIONALIZED POPULATION
#               AS REFERENCED IN FIGURE 1 OF THE MEPS STAT BRIEF #193
#
#               THE PROGRAM USES 2005 MEPS DATA.
#
#               TYPE OF SERVICE CATEGORIES:
#                 HOSPITAL INPATIENT
#                 OFFICE-BASED VISITS
#                 PRESCRIBED MEDICINES
#                 HOSPITAL OUTPATIENT
#                 DENTAL VISITS
#                 EMERGENCY ROOM
#                 HOME HEALTH CARE (AGENCY & NON-AGENCY)
#                 OTHER (TOTAL EXPENDITURES - ABOVE EXPENDITURE CATEGORIES)
#
#               NOTE: EXPENSES INCLUDE BOTH FACILITY AND PHYSICIAN EXPENSES.
#
# INPUT FILE:   C:\MEPS\DATA\H97.SAS7BDAT (2005 FULL-YEAR FILE)
#
# Python port of SAS/older_exercises_1996_to_2006/Estimation_examples/E6/E6.sas
# ----------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import load_meps, svy_mean, svy_ratio, svy_total  # noqa: E402

pd.set_option("display.width", 200)

# Read in data from 2005 Consolidated Data File (HC-097)
cols = ["TOTEXP05", "IPDEXP05", "IPFEXP05", "OBVEXP05", "RXEXP05", "OPDEXP05", "OPFEXP05", "DVTEXP05", "ERDEXP05",
        "ERFEXP05", "HHAEXP05", "HHNEXP05", "OTHEXP05", "VARSTR", "VARPSU", "PERWT05F"]  # fmt: skip
PUF97 = load_meps("h97", columns=cols)
PUF97 = PUF97[PUF97["PERWT05F"] > 0].copy()

# Define expenditure variables by type of service
P = PUF97
P["TOTAL"] = P["TOTEXP05"]
P["HOSPITAL_INPATIENT"] = P["IPDEXP05"] + P["IPFEXP05"]
P["OFFICE_BASED"] = P["OBVEXP05"]
P["PRESCRIBED_MEDICINES"] = P["RXEXP05"]
P["HOSPITAL_OUTPATIENT"] = P["OPDEXP05"] + P["OPFEXP05"]
P["DENTAL"] = P["DVTEXP05"]
P["EMERGENCY_ROOM"] = P["ERDEXP05"] + P["ERFEXP05"]
P["HOME_HEALTH"] = P["HHAEXP05"] + P["HHNEXP05"]
services = ["HOSPITAL_INPATIENT", "OFFICE_BASED", "PRESCRIBED_MEDICINES", "HOSPITAL_OUTPATIENT", "DENTAL",
            "EMERGENCY_ROOM", "HOME_HEALTH"]  # fmt: skip
P["OTHER"] = P["TOTAL"] - P[services].sum(axis=1)
services.append("OTHER")

# Create flag (1/0) variables for persons with an expense, by type of service
flags = []
for v in ["TOTAL"] + services:
    flag = "X_ANYSVCE" if v == "TOTAL" else f"X_{v}"
    P[flag] = np.where(P[v] > 0, 1, 0)
    flags.append(flag)

design = dict(weight="PERWT05F", strata="VARSTR", psu="VARPSU")


def mean_sum(df, variables):
    """PROC SURVEYMEANS MEAN STDERR SUM STD."""
    return svy_mean(df, variables, **design).merge(
        svy_total(df, variables, **design), on=["variable", "n"], suffixes=("_mean", "_sum")
    )


print("EXPENDITURES BY TYPE OF SERVICE, 2005")
print(mean_sum(P, services + ["TOTAL"]).to_string(index=False), "\n")
print("PERCENTAGE DISTRIBUTION OF SPENDING (RATIO <service> / TOTAL)")
print(svy_ratio(P, services, "TOTAL", **design).to_string(index=False), "\n")
print("PERSONS WITH AN EXPENSE, BY TYPE OF SERVICE")
print(mean_sum(P, flags).to_string(index=False))
