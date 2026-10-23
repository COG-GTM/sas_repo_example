"""Python port of SAS/workshop_exercises/exercise_4c/Exercise4c.sas

 This program illustrates how to pool MEPS data files from different years. It
 highlights one example of a discontinuity that may be encountered when
 working with data from before and after the 2018 CAPI re-design.


 The program pools 2017 and 2018 data and calculates for the civilian noninstitutionized population:
  - Percentage of people with Joint Pain / Arthritis (JTPAIN**, ARTHDX)
  - Average expenditures per person, by Joint Pain status (TOTEXP, TOTSLF)

 Notes:
  - Variables with year-specific names must be renamed before combining files
    (e.g. 'TOTEXP17' and 'TOTEXP18' renamed to 'totexp')

Input files:
  - 2017 Full-year consolidated file
  - 2018 Full-year consolidated file

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# PROC FORMAT
yes_no_fmt = {1: "Yes", 2: "No"}


def prep(name, yy, jtpain):
    """One year of FYC data: pooled weight, analysis subpopulation (SPOP) and JOINT_PAIN."""
    d = read_meps(name, ["VARSTR", "VARPSU", f"PERWT{yy}F", "AGELAST", "ARTHDX", jtpain, f"TOTEXP{yy}", f"TOTSLF{yy}"])
    d = d.rename(columns={f"TOTEXP{yy}": "TOTEXP", f"TOTSLF{yy}": "TOTSLF", jtpain: "JTPAIN"})
    d["PERWTF"] = d[f"PERWT{yy}F"] / 2
    adult = (d.AGELAST >= 18) & ~((d.ARTHDX <= 0) & (d.JTPAIN < 0))
    d["SPOP"] = np.where(adult, 1, 2)
    d["JOINT_PAIN"] = np.where(adult, np.where((d.ARTHDX == 1) | (d.JTPAIN == 1), 1, 2), np.nan)
    return d


# Pool 2017 and 2018 (SAS: SET meps_2017 (RENAME=...) meps_2018 (RENAME=...)) ------
MEPS_1718 = pd.concat([prep("h201", 17, "JTPAIN31"), prep("h209", 18, "JTPAIN31_M18")], ignore_index=True)
MEPS_1718["TOTEXP_X"] = MEPS_1718.TOTEXP
print(freq_list(MEPS_1718, ["ARTHDX", "JTPAIN", "JOINT_PAIN"]))
print(freq_list(MEPS_1718, ["ARTHDX", "JTPAIN", "SPOP"]))
print(freq_list(MEPS_1718, ["SPOP"]))
print(freq_list(MEPS_1718, ["JOINT_PAIN"]))

# Percent of adults with joint pain: CLASS JOINT_PAIN, DOMAIN SPOP('1') --------------
MEPS_1718["JOINT_PAIN_F"] = sas_format(MEPS_1718.JOINT_PAIN, yes_no_fmt, missing="(not in SPOP)")
jp = surveyfreq(MEPS_1718, ["SPOP", "JOINT_PAIN_F"], weight="PERWTF", strata="VARSTR", cluster="VARPSU")
jp["Mean"], jp["StdErr"] = jp.RowPercent / 100, jp.RowStdErr / 100
show("JOINT_PAIN, SPOP = 1", jp[jp.SPOP == 1][["JOINT_PAIN_F", "Frequency", "Mean", "StdErr", "WgtFreq", "StdDev"]])

# Mean and total expenses, DOMAIN SPOP('1')*JOINT_PAIN --------------------------------
ex = surveymeans(MEPS_1718, ["TOTEXP", "TOTSLF"], weight="PERWTF", strata="VARSTR", cluster="VARPSU",
                 domain=["SPOP", "JOINT_PAIN_F"])
show("Expenses by JOINT_PAIN, SPOP = 1", ex[ex.SPOP == 1][["JOINT_PAIN_F", "VarName", "N", "Mean", "StdErr", "Sum", "StdDev"]])
