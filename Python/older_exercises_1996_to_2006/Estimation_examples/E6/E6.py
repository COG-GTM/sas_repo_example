"""Python port of SAS/older_exercises_1996_to_2006/Estimation_examples/E6/E6.sas

PROGRAM:      <MEPS_DIR>/PROG/E6.SAS

DESCRIPTION:  THIS PROGRAM GENERATES THE FOLLOWING ESTIMATES FOR 2005
              PERCENTAGE DISTRIBUTION OF HEALTH CARE SPENDING, BY TYPE OF SERVICE,
              FOR THE U.S. CIVILIAN NONINSTITUTIONALIZED POPULATION
              AS REFERENCED IN FIGURE 1 OF THE MEPS STAT BRIEF #193
              THE PROGRAM USES 2005 MEPS DATA.

              TYPE OF SERVICE CATEGORIES:
                HOSPITAL INPATIENT
                OFFICE-BASED VISITS
                PRESCRIBED MEDICINES
                HOSPITAL OUTPATIENT
                DENTAL VISITS
                EMERGENCY ROOM
                HOME HEALTH CARE (AGENCY & NON-AGENCY)
                OTHER (TOTAL EXPENDITURES - ABOVE EXPENDITURE CATEGORIES)

              NOTE: EXPENSES INCLUDE BOTH FACILITY AND PHYSICIAN EXPENSES.

INPUT FILE:   <MEPS_DIR>/DATA/H97.SAS7BDAT (2005 FULL-YEAR FILE)


;

FOOTNOTE 'PROGRAM: <MEPS_DIR>/PROG/E6.SAS';

LIBNAME CDATA '<MEPS_DIR>/DATA';

TITLE1 'AHRQ MEPS DATA USERS WORKSHOP -- SEPTEMBER 2008';


Read in data from 2005 Consolidated Data File (HC-097)

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# 2005 full-year consolidated file (HC-097), persons with a positive weight ------------
PUF97 = read_meps("h97", ["TOTEXP05", "IPDEXP05", "IPFEXP05", "OBVEXP05", "RXEXP05", "OPDEXP05", "OPFEXP05", "DVTEXP05",
                          "ERDEXP05", "ERFEXP05", "HHAEXP05", "HHNEXP05", "OTHEXP05", "VARSTR", "VARPSU", "PERWT05F"])
PUF97 = PUF97[PUF97.PERWT05F > 0].copy()

# Expenditures by type of service
PUF97["TOTAL"] = PUF97.TOTEXP05
PUF97["HOSPITAL_INPATIENT"] = PUF97.IPDEXP05 + PUF97.IPFEXP05
PUF97["OFFICE_BASED"] = PUF97.OBVEXP05
PUF97["PRESCRIBED_MEDICINES"] = PUF97.RXEXP05
PUF97["HOSPITAL_OUTPATIENT"] = PUF97.OPDEXP05 + PUF97.OPFEXP05
PUF97["DENTAL"] = PUF97.DVTEXP05
PUF97["EMERGENCY_ROOM"] = PUF97.ERDEXP05 + PUF97.ERFEXP05
PUF97["HOME_HEALTH"] = PUF97.HHAEXP05 + PUF97.HHNEXP05
TOS = ["HOSPITAL_INPATIENT", "OFFICE_BASED", "PRESCRIBED_MEDICINES", "HOSPITAL_OUTPATIENT", "DENTAL", "EMERGENCY_ROOM",
       "HOME_HEALTH"]
PUF97["OTHER"] = PUF97.TOTAL - PUF97[TOS].sum(axis=1)
TOS += ["OTHER"]

# ARRAY / DO loop: 1/0 flags for persons with an expense
FLAGS = []
for v in ["TOTAL"] + TOS:
    x = "X_ANYSVCE" if v == "TOTAL" else f"X_{v}"
    PUF97[x] = np.where(PUF97[v] > 0, 1, 0)
    FLAGS.append(x)

design = dict(weight="PERWT05F", strata="VARSTR", cluster="VARPSU")
COLS = ["VarName", "Mean", "StdErr", "Sum", "StdDev"]
show("Expenditures by type of service (MEAN STDERR SUM STD)", surveymeans(PUF97, TOS + ["TOTAL"], **design)[COLS])
show("RATIO type of service / TOTAL", pd.concat([surveyratio(PUF97, v, "TOTAL", **design) for v in TOS], ignore_index=True))
show("Persons with an expense, by type of service", surveymeans(PUF97, FLAGS, **design)[COLS])
