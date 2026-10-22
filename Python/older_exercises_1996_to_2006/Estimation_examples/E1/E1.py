# ----------------------------------------------------------------
# PROGRAM:       C:\MEPS\PROG\EXAMPLE_E1.SAS
#
# DESCRIPTION:   THIS EXAMPLE SHOWS HOW TO COMPUTE PERSON-LEVEL
#                ESTIMATES FOR PERSON-LEVEL HEALTHCARE EXPENDITURES.
#                ESTIMATES INCLUDE: MEANS, PROPORTIONS, TOTALS.
#
# INPUT FILE:    C:\MEPS\DATA\H60.SAS7BDAT (2001 FULL-YEAR DATA FILE)
#                      -- PERSON-LEVEL FILE
#
# Python port of SAS/older_exercises_1996_to_2006/Estimation_examples/E1/E1.sas
# ----------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import add_cv_clm, apply_format, design_df, load_meps, svy_glm, svy_mean, svy_total  # noqa: E402

pd.set_option("display.width", 200)

# Read the required variables from the full-year file (HC-060). AGE is AGE01X,
# or, if AGE01X is missing, AGE42X or AGE31X. ANY_EXP indicates zero (0) or
# some (100) healthcare expenses in 2001; '100' lets the mean read as a percentage.
H60 = load_meps("h60", columns=["TOTEXP01", "SEX", "AGE01X", "AGE42X", "AGE31X", "PERWT01F", "VARPSU01", "VARSTR01"])
H60["AGE"] = np.select([H60["AGE01X"] >= 0, H60["AGE42X"] >= 0, H60["AGE31X"] >= 0],
                       [H60["AGE01X"], H60["AGE42X"], H60["AGE31X"]], default=H60["AGE01X"])  # fmt: skip
H60["ANY_EXP"] = np.where(H60["TOTEXP01"] == 0, 0, 100)  # SOME EXPENSES IN 2001

# Formats for age and sex are used to create categories for the analysis
AGEF = [(0, 64, "0-64"), (65, 90, "65-90")]
SEXF = {1: "MALE", 2: "FEMALE"}
H60["AGEF"] = apply_format(H60["AGE"], AGEF)
H60["SEXF"] = apply_format(H60["SEX"], SEXF)

design = dict(weight="PERWT01F", strata="VARSTR01", psu="VARPSU01")
dof = design_df(H60, "VARSTR01", "VARPSU01")


def means(var):
    """PROC SURVEYMEANS NOBS SUMWGT MEAN STDERR CV CLM; VAR var; DOMAIN AGE SEX."""
    print(add_cv_clm(svy_mean(H60, var, **design), dof).to_string(index=False))
    for dom in ("AGEF", "SEXF"):
        print(add_cv_clm(svy_mean(H60, var, domain=dom, **design), dof).to_string(index=False))
    print()


# MEANS -----------------------------------------------------------------------
print("MEANS: 2001 EXPENSES (TOTEXP01)")
means("TOTEXP01")

# PROC SURVEYREG: recode SEX so that FEMALE = 0 and MALE = 1 ------------------
H60V2 = H60.assign(SEX=H60["SEX"].replace({2: 0}))
print("PROC SURVEYREG: TOTEXP01 = SEX")
print(svy_glm("TOTEXP01 ~ SEX", H60V2, family=sm.families.Gaussian(), **design).summary_frame().to_string(), "\n")

# PROPORTIONS: mean of the dichotomous variable ANY_EXP (0/100) ----------------
print("PROPORTIONS: SOME EXPENSES IN 2001 (ANY_EXP)")
means("ANY_EXP")

# TOTALS: SUM requests the total for TOTEXP01; SUMWGT the population estimate --
print("TOTALS: TOTEXP01")
print(add_cv_clm(svy_total(H60, "TOTEXP01", **design), dof).to_string(index=False))
print(f"Sum of weights (population): {H60['PERWT01F'].sum():,.0f}")
