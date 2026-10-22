# ----------------------------------------------------------------
# PROGRAM:       C:\MEPS\PROG\EXAMPLE_E3.SAS
#
# DESCRIPTION:   THIS EXAMPLE SHOWS HOW TO CREATE A LONGITUDINAL
#                FILE FOR 1999-2000 (PANEL 4) AND COMPUTE PERSON-
#                LEVEL ESTIMATES FOR INSURANCE COVERAGE AND
#                EXPENDITURES.
#
# VARIABLES:     UNINS99, UNINS00
#                   (INSURED ALL OF 1999, 2000)
#                TOTEXP99, TOTEXP00
#                   (TOTAL HEALTHCARE EXPENDITURES, 1999)
#
# INPUT FILE:    (1) C:\MEPS\DATA\H50.SAS7BDAT (2000 FULL-YEAR DATA FILE)
#                (3) C:\MEPS\DATA\H38.SAS7BDAT (1999 FULL-YEAR DATA FILE)
#                (5) C:\MEPS\DATA\H58.SAS7BDAT
#                                 (PANEL 4 LONGITUDINAL WEIGHT FILE)
#
# Python port of SAS/older_exercises_1996_to_2006/Estimation_examples/E3/E3.sas
# ----------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import add_cv_clm, design_df, load_meps, svy_mean  # noqa: E402

pd.set_option("display.width", 200)

# Use the PANELyy variable to pick out panel 4 persons
H38 = load_meps("h38", columns=["DUPERSID", "UNINS99", "TOTEXP99", "PANEL99"])
H38 = H38[H38["PANEL99"] == 4].drop(columns="PANEL99")
H50 = load_meps("h50", columns=["DUPERSID", "UNINS00", "TOTEXP00", "PANEL00"])
H50 = H50[H50["PANEL00"] == 4].drop(columns="PANEL00")

# Panel 4 longitudinal file; YRINDP4 = 1 selects persons in both 1999 and 2000
# (current releases of HC-058 name these LONGWT / YEARIND)
H58 = load_meps("h58", columns=["DUPERSID", "LONGWTP4", "LONGWT", "VARPSUP4", "VARSTRP4", "YRINDP4", "YEARIND"])
H58 = H58.rename(columns={"LONGWT": "LONGWTP4", "YEARIND": "YRINDP4"})
H58 = H58[H58["YRINDP4"] == 1].drop(columns="YRINDP4")

# 1999 analytic variables are re-coded 1 or 0; 2000 variables 100 or 0 so the
# SURVEYMEANS 'MEAN' reads as a percent. '2' [NO] values of UNINSyy become 0.
LONGP4 = H38.merge(H50, on="DUPERSID").merge(H58, on="DUPERSID")
LONGP4["UNINS99"] = np.where(LONGP4["UNINS99"] == 1, 1, 0)
LONGP4["UNINS00"] = np.where(LONGP4["UNINS00"] == 1, 100, 0)
LONGP4["TOTEXP99"] = np.where(LONGP4["TOTEXP99"] > 0, 1, 0)
LONGP4["TOTEXP00"] = np.where(LONGP4["TOTEXP00"] > 0, 100, 0)

# PROC FREQ ... WEIGHT LONGWTP4 (weighted, non-survey) ------------------------
for v in (1, 0):
    sub = LONGP4[LONGP4["TOTEXP99"] == v]
    tab = sub.groupby("TOTEXP00")["LONGWTP4"].sum().rename("FREQUENCY").to_frame()
    tab["PERCENT"] = 100 * tab["FREQUENCY"] / tab["FREQUENCY"].sum()
    print(f"TOTEXP00 WHERE TOTEXP99 = {v} (WEIGHT LONGWTP4)")
    print(tab, "\n")

design = dict(weight="LONGWTP4", strata="VARSTRP4", psu="VARPSUP4")
dof = design_df(LONGP4, "VARSTRP4", "VARPSUP4")
for var, dom in (("UNINS00", "UNINS99"), ("TOTEXP00", "TOTEXP99")):
    print(f"PROC SURVEYMEANS: {var} by DOMAIN {dom}")
    print(add_cv_clm(svy_mean(LONGP4, var, domain=dom, **design), dof).to_string(index=False), "\n")
