"""Python port of SAS/workshop_exercises/exercise_6a/Exercise6.sas

This program includes a regression example for persons receiving a flu shot
in the last 12 months for the civilian noninstitutionized population, including:
- Percentage of people with a flu shot (civilian noninstitutionized population), 2018:
- Logistic regression: to identify demographic factors associated with receiving a flu shot

 Input file:
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
age18p_f = {slice(18, None): "18+"}  # other = '0-17'
sex_fmt = {1: "Male", 2: "Female"}
racethx_fmt = {1: "Hispanic", 2: "NH White only", 3: "NH Black only", 4: "NH Asian only", 5: "NH Other etc"}
INSCOV18_fmt = {1: "Any Private", 2: "Public Only", 3: "Uninsured"}

meps_2018 = read_meps("h209", ["VARSTR", "VARPSU", "PERWT18F", "SAQWT18F", "ADFLST42", "AGELAST", "RACETHX",
                               "POVCAT18", "INSCOV18", "SEX"])
meps_2018["FLUSHOT"] = np.select([meps_2018.ADFLST42 == 1, meps_2018.ADFLST42 == 2], [1, 0], np.nan)

# Percent of adults with a flu shot (DOMAIN AGELAST('18+'), SAQ weight) ----------------
meps_2018["AGE18P"] = sas_format(meps_2018.AGELAST, age18p_f, other="0-17")
m = surveymeans(meps_2018, "FLUSHOT", weight="SAQWT18F", strata="VARSTR", cluster="VARPSU", domain="AGE18P")
show("Statistics for AGELAST domains", m[m.AGE18P == "18+"][["AGE18P", "VarName", "N", "Mean", "StdErr"]])

# PROC SURVEYLOGISTIC: model FLUSHOT(ref='0') = AGELAST SEX RACETHX INSCOV18 -----------
# CLASS SEX(ref='Male') RACETHX(ref='Hispanic') INSCOV18(ref='Any Private') / PARAM=REF
for v, f in [("SEX", sex_fmt), ("RACETHX", racethx_fmt), ("INSCOV18", INSCOV18_fmt)]:
    meps_2018[v + "_F"] = sas_format(meps_2018[v], f)
fit = surveyreg(meps_2018, "FLUSHOT ~ AGELAST + C(SEX_F, Treatment('Male')) + C(RACETHX_F, Treatment('Hispanic'))"
                " + C(INSCOV18_F, Treatment('Any Private'))", weight="SAQWT18F", strata="VARSTR", cluster="VARPSU",
                family="logistic")
show("Analysis of maximum likelihood estimates (probability modeled is FLUSHOT=1)", fit)
