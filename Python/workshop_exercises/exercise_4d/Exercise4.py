"""Python port of SAS/workshop_exercises/exercise_4d/Exercise4.sas

PROGRAM:      EXERCISE4.SAS

 This program illustrates how to pool MEPS data files from different years. It
 highlights one example of a discontinuity that may be encountered when
 working with data from before and after the 2018 MEPS CAPI re-design.

 The program pools 2017, 2018 and 2019 data and calculates
  - percentage of people with Joint Pain / Arthritis (JTPAIN**, ARTHDX)
  - average expenditures per person, by Joint Pain status (TOTEXP, TOTSLF)
  - standard errors by specifying common variance structure when pooling data.
 for the U.S. civilian noninstitutionized population.

 Input files:
  - 2017 Full-year consolidated file
  - 2018 Full-year consolidated file
  - 2019 Full-year consolidated file
  - 1996-2019 pooled linkage variance estimation file

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


def prep(name, yy, jtpain, year):
    d = read_meps(name, ["DUPERSID", "PANEL", "VARSTR", "VARPSU", f"PERWT{yy}F", "AGELAST", "ARTHDX", jtpain,
                         f"TOTEXP{yy}", f"TOTSLF{yy}"])
    d = d.rename(columns={f"TOTEXP{yy}": "TOTEXP", f"TOTSLF{yy}": "TOTSLF", jtpain: "JTPAIN"})
    d["YEAR"] = year
    d["PERWTF"] = d[f"PERWT{yy}F"] / 3
    if year == 2017:
        adult = (d.AGELAST >= 18) & ~((d.ARTHDX <= 0) & (d.JTPAIN < 0))
        # As in the SAS code, the 2017 DUPERSID is prefixed with PANEL only inside the SPOP=1 block
        d["DUPERSID"] = np.where(adult, d.PANEL.astype(int).astype(str) + d.DUPERSID.str.strip(), "")
    else:
        adult = (d.AGELAST >= 18) & ~((d.ARTHDX < 0) & (d.JTPAIN < 0))
    d["SPOP"] = np.where(adult, 1, 0)
    d["JOINT_PAIN"] = np.where(adult, np.where((d.ARTHDX == 1) | (d.JTPAIN == 1), 1, 2), np.nan)
    return d


# Concatenate 2017, 2018 and 2019 FYC files ----------------------------------------
MEPS_171819 = pd.concat([prep("h201", 17, "JTPAIN31", 2017), prep("h209", 18, "JTPAIN31_M18", 2018),
                         prep("h216", 19, "JTPAIN31_M18", 2019)], ignore_index=True)
MEPS_171819["ZERO_WEIGHT"] = np.where(MEPS_171819.PERWTF == 0, 1, 0)

# Pooled linkage variance estimation file (HC-036): pad 8-character DUPERSIDs with PANEL
VS = read_meps("h36u19")
VS["DUPERSID"] = np.where(VS.DUPERSID.str.strip().str.len() == 8,
                          VS.PANEL.astype(int).map("{:02d}".format) + VS.DUPERSID.str.strip(), VS.DUPERSID.str.strip())
VS = VS[VS.PANEL.isin([21, 22, 23, 24])].drop_duplicates("DUPERSID")

# MERGE ... (IN=a); IF a  -> left join
M = MEPS_171819.merge(VS[["DUPERSID", "STRA9619", "PSU9619"]], on="DUPERSID", how="left")

pos = M[(M.PERWTF > 0)]
print(freq_list(M, ["ZERO_WEIGHT"]))
print(freq_list(pos, ["SPOP", "JOINT_PAIN"]))
print(pos[pos.SPOP == 1][["STRA9619", "PSU9619"]].agg(["count", lambda s: s.isna().sum(), "min", "max"]))

# SAS drops records with missing design variables (STRA9619/PSU9619) from the analysis;
# none of them are in SPOP = 1.
M = M[M.STRA9619.notna() & M.PSU9619.notna()]
M["JOINT_PAIN_F"] = sas_format(M.JOINT_PAIN, yes_no_fmt, missing="(not in SPOP)")

jp = surveyfreq(M, ["SPOP", "JOINT_PAIN_F"], weight="PERWTF", strata="STRA9619", cluster="PSU9619")
jp["Mean"], jp["StdErr"] = jp.RowPercent / 100, jp.RowStdErr / 100
show("JOINT_PAIN, SPOP = 1 (pooled 2017-2019)", jp[jp.SPOP == 1][["JOINT_PAIN_F", "Frequency", "Mean", "StdErr", "WgtFreq", "StdDev"]])

ex = surveymeans(M, ["TOTEXP", "TOTSLF"], weight="PERWTF", strata="STRA9619", cluster="PSU9619",
                 domain=["SPOP", "JOINT_PAIN_F"])
show("Expenses by JOINT_PAIN, SPOP = 1 (pooled 2017-2019)",
     ex[ex.SPOP == 1][["JOINT_PAIN_F", "VarName", "N", "Mean", "StdErr", "Sum", "StdDev"]])
