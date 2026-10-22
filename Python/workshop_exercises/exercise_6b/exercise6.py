# -----------------------------------------------------------------------------
# program:      exercise6.sas
#
# this program includes an example of 3 logistic regression models, each with a
# sepatate dependent variable.
#
# cvdlayca53 - delay med care for covid r5/3 - recoded to yes/no (1,0)
# cvdlaypm53 - delay getting pmed for covid r5/3 - recoded to yes/no (1,0)
# cvdlaydn53 - delay getting dental for covid r5/3 - recoded to yes/no (1,0)
#
# covariates: age, gender, race/ethnicity, health insurance coverage status,
# and region
#
# the program also estimate the propotion of persons with delayed care events
# in the civilian noninstitutionized population.
#
# input file: 2020 full-year consolidated file (h224)
#
# Python port of SAS/workshop_exercises/exercise_6b/exercise6.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, load_meps, svy_glm, svy_mean  # noqa: E402

pd.set_option("display.width", 200)

# Formats (PROC FORMAT) -------------------------------------------------------

SEX_FMT = {1: "1. male", 2: "2. female"}
REGION_FMT = {1: "1. northeast", 2: "2. midwest", 3: "3. south", 4: "4. west"}
INSCOV20_FMT = {1: "1. any private", 2: "2. public only", 3: "3. uninsured"}
RACETHX_FMT = {1: "1. hispanic", 2: "2. nh white only", 3: "3. nh black only", 4: "4. nh asian only",
               5: "5. nh other etc"}  # fmt: skip

kept_vars_2020 = ["VARSTR", "VARPSU", "PERWT20F", "CVDLAYCA53", "CVDLAYPM53", "CVDLAYDN53", "AGELAST", "SEX",
                  "RACETHX", "POVCAT20", "INSCOV20", "REGION53"]  # fmt: skip
meps_2020 = load_meps("h224", columns=kept_vars_2020)
meps_2020["REGION"] = meps_2020["REGION53"].where(meps_2020["REGION53"] != -1)  # region recode

# 1 -> 1 (yes), 2 -> 0 (no), negative -> missing
recode = {"CVDLAYCA53": "DELAYED_CARE_MED", "CVDLAYDN53": "DELAYED_CARE_DENTAL", "CVDLAYPM53": "DELAYED_CARE_PMEDS"}
for x, y in recode.items():
    meps_2020[y] = np.select([meps_2020[x] == 1, meps_2020[x] == 2], [1.0, 0.0], default=np.nan)
outcomes = list(recode.values())

print("Proportion of persons with delayed care events, 2020")
print(svy_mean(meps_2020, outcomes, weight="PERWT20F", missing=False).to_string(index=False), "\n")

meps_2020["SEX"] = apply_format(meps_2020["SEX"], SEX_FMT)
meps_2020["RACETHX"] = apply_format(meps_2020["RACETHX"], RACETHX_FMT)
meps_2020["INSCOV20"] = apply_format(meps_2020["INSCOV20"], INSCOV20_FMT)
meps_2020["REGION"] = apply_format(meps_2020["REGION"], REGION_FMT)

covariates = (
    "AGELAST + C(SEX, Treatment('1. male')) + C(RACETHX, Treatment('1. hispanic'))"
    " + C(INSCOV20, Treatment('1. any private')) + C(REGION, Treatment('1. northeast'))"
)
for y in outcomes:
    data = meps_2020.dropna(subset=[y])
    data = data[data["REGION"] != "Missing"]
    model = svy_glm(f"{y} ~ {covariates}", data, weight="PERWT20F", family=sm.families.Binomial())
    print(f"Survey logistic regression: {y} (ref = 0)")
    print(model.summary_frame().to_string(), "\n")
