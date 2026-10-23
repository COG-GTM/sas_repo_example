"""Python port of SAS/workshop_exercises/exercise_6b/exercise6.sas

program:      exercise6.sas
this program includes an example of 3 logistic regression models, each with a
sepatate dependent variable.


cvdlayca53 - delay med care for covid r5/3 - recoded to yes/no (1,0)
cvdlaypm53 - delay getting pmed for covid r5/3 - recoded to yes/no (1,0)
cvdlaydn53 - delay getting dental for covid r5/3 - recoded to yes/no (1,0)

covariates: age, gender, race/ethnicity, health insurance coverage status, and region

the program also estimate the propotion of persons with delayed care events
in the civilian noninstitutionized population.

input file: 2020 full-year consolidated file

clear log, output, and odsresults from the previous run automatically

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# PROC FORMAT
sex_fmt = {1: "1. male", 2: "2. female"}
region_fmt = {1: "1. northeast", 2: "2. midwest", 3: "3. south", 4: "4. west"}
inscov20_fmt = {1: "1. any private", 2: "2. public only", 3: "3. uninsured"}
racethx_fmt = {1: "1. hispanic", 2: "2. nh white only", 3: "3. nh black only", 4: "4. nh asian only",
               5: "5. nh other etc"}

meps_2020 = read_meps("h224", ["VARSTR", "VARPSU", "PERWT20F", "CVDLAYCA53", "CVDLAYPM53", "CVDLAYDN53", "AGELAST",
                               "SEX", "RACETHX", "POVCAT20", "INSCOV20", "REGION53"])
meps_2020["REGION"] = meps_2020.REGION53.where(meps_2020.REGION53 != -1)

# SAS ARRAY recode: 1 -> 1, 2 -> 0, negative -> missing
for x, y in [("CVDLAYCA53", "DELAYED_CARE_MED"), ("CVDLAYDN53", "DELAYED_CARE_DENTAL"),
             ("CVDLAYPM53", "DELAYED_CARE_PMEDS")]:
    meps_2020[y] = np.select([meps_2020[x] == 1, meps_2020[x] == 2], [1, 0], np.nan)

yvars = ["DELAYED_CARE_MED", "DELAYED_CARE_DENTAL", "DELAYED_CARE_PMEDS"]
show("Percent of persons who delayed care because of COVID-19, 2020",
     surveymeans(meps_2020, yvars, weight="PERWT20F", strata="VARSTR", cluster="VARPSU")[["VarName", "N", "Mean", "StdErr"]])

for v, f in [("SEX", sex_fmt), ("RACETHX", racethx_fmt), ("INSCOV20", inscov20_fmt), ("REGION", region_fmt)]:
    meps_2020[v + "_F"] = sas_format(meps_2020[v], f)
rhs = ("AGELAST + C(SEX_F, Treatment('1. male')) + C(RACETHX_F, Treatment('1. hispanic'))"
       " + C(INSCOV20_F, Treatment('1. any private')) + C(REGION_F, Treatment('1. northeast'))")
for y in yvars:
    fit = surveyreg(meps_2020, f"{y} ~ {rhs}", weight="PERWT20F", strata="VARSTR", cluster="VARPSU", family="logistic")
    show(f"PROC SURVEYLOGISTIC: {y} (ref='0')", fit)
