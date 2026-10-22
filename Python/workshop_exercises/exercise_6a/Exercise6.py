# -----------------------------------------------------------------------------
# This program includes a regression example for persons receiving a flu shot
# in the last 12 months for the civilian noninstitutionized population,
# including:
#  - Percentage of people with a flu shot (civilian noninstitutionized
#    population), 2018
#  - Logistic regression: to identify demographic factors associated with
#    receiving a flu shot
#
#  Input file:
#   - 2018 Full-year consolidated file (h209)
#
# Python port of SAS/workshop_exercises/exercise_6a/Exercise6.sas
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

AGE18P_F = [(18, np.inf, "18+")]  # other = '0-17'
SEX_FMT = {1: "Male", 2: "Female"}
RACETHX_FMT = {1: "Hispanic", 2: "NH White only", 3: "NH Black only", 4: "NH Asian only", 5: "NH Other etc"}
INSCOV18_FMT = {1: "Any Private", 2: "Public Only", 3: "Uninsured"}

kept_vars_2018 = ["VARSTR", "VARPSU", "PERWT18F", "SAQWT18F", "ADFLST42", "AGELAST", "RACETHX", "POVCAT18",
                  "INSCOV18", "SEX"]  # fmt: skip
meps_2018 = load_meps("h209", columns=kept_vars_2018)
meps_2018["FLUSHOT"] = meps_2018["ADFLST42"].map({1: 1.0, 2: 0.0})  # all other values -> missing

# Percentage of adults with a flu shot (DOMAIN agelast('18+')) -----------------
print("Percentage of people with a flu shot, 2018 (adults, SAQ weight)")
meps_2018["AGE18P"] = apply_format(meps_2018["AGELAST"], AGE18P_F, default="0-17")
res = svy_mean(meps_2018, "FLUSHOT", weight="SAQWT18F", domain="AGE18P", missing=False)
print(res[res["AGE18P"] == "18+"].to_string(index=False), "\n")

# PROC SURVEYLOGISTIC: flushot(ref='0') = agelast sex racethx inscov18 --------
#  CLASS sex (ref='Male') RACETHX (ref='Hispanic') INSCOV18 (ref='Any Private') / param=ref
meps_2018["SEX"] = apply_format(meps_2018["SEX"], SEX_FMT)
meps_2018["RACETHX"] = apply_format(meps_2018["RACETHX"], RACETHX_FMT)
meps_2018["INSCOV18"] = apply_format(meps_2018["INSCOV18"], INSCOV18_FMT)

formula = (
    "FLUSHOT ~ AGELAST + C(SEX, Treatment('Male')) + C(RACETHX, Treatment('Hispanic'))"
    " + C(INSCOV18, Treatment('Any Private'))"
)
model = svy_glm(formula, meps_2018.dropna(subset=["FLUSHOT"]), weight="SAQWT18F", family=sm.families.Binomial())
print("Survey logistic regression for receiving a flu shot, 2018")
print(model.summary_frame().to_string())
