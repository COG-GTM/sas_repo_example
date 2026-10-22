# -----------------------------------------------------------------------------
# This program generates the following estimates on national health care
# expenses for the civilian noninstitutionized population, 2018:
#   - Overall expenses (National totals)
#   - Percentage of persons with an expense
#   - Mean expense per person
#   - Mean/median expense per person with an expense:
#     - Mean expense per person with an expense
#     - Mean expense per person with an expense, by age group
#     - Median expense per person with an expense, by age group
#
# Input file:
#  - 2018 Full-year consolidated file (h209)
#
# Python port of SAS/workshop_exercises/exercise_1c/Exercise1c.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, load_meps, svy_freq, svy_mean, svy_quantile, svy_total  # noqa: E402

pd.set_option("display.width", 200)

# Formats (PROC FORMAT) -------------------------------------------------------

AGECAT = [(-np.inf, 64, "0-64"), (65, np.inf, "65+")]
TOTEXP18_CATE = {0: "No Expense"}  # Other = 'Any Expense'

# READ IN DATA FROM 2018 CONSOLIDATED DATA FILE (HC-209) ----------------------

PUF209 = load_meps("h209", columns=["TOTEXP18", "AGELAST", "VARSTR", "VARPSU", "PERWT18F", "PANEL"])
PUF209["WITH_AN_EXPENSE"] = PUF209["TOTEXP18"]  # Create another version of TOTEXP18
# Create a character variable based on a numeric variable using a table lookup
PUF209["CHAR_WITH_AN_EXPENSE"] = apply_format(PUF209["TOTEXP18"], TOTEXP18_CATE, default="Any Expense")

print("Variables in PUF209 (PROC CONTENTS):")
print(PUF209.dtypes.to_string(), "\n")

print("MEPS FULL-YEAR CONSOLIDATED FILE, 2018")
design = dict(weight="PERWT18F", strata="VARSTR", psu="VARPSU")

# PERCENTAGE OF PERSONS WITH AN EXPENSE, 2018 - Method 1 -----------------------
#  (CLASS variable formatted with TOTEXP18_CATE.)
print("\nPERCENTAGE OF PERSONS WITH AN EXPENSE, 2018 - Method 1")
fmt = PUF209.assign(
    WITH_AN_EXPENSE=apply_format(PUF209["WITH_AN_EXPENSE"], TOTEXP18_CATE, default="Any Expense")
)
print(svy_freq(fmt, "WITH_AN_EXPENSE", **design).to_string(index=False))

# PERCENTAGE OF PERSONS WITH AN EXPENSE, 2018 - Method 2 -----------------------
#  (character variable)
print("\nPERCENTAGE OF PERSONS WITH AN EXPENSE, 2018 - Method 2")
print(svy_freq(PUF209, "CHAR_WITH_AN_EXPENSE", **design).to_string(index=False))

# PERCENTAGE OF PERSONS WITH AN EXPENSE, 2018 - Method 3 (PROC SURVEYFREQ) -----
print("\nPERCENTAGE OF PERSONS WITH AN EXPENSE, 2018 - Method 3")
print(svy_freq(PUF209, "CHAR_WITH_AN_EXPENSE", **design).to_string(index=False))

# MEAN AND MEDIAN EXPENSE PER PERSON WITH AN EXPENSE, OVERALL and FOR AGES 0-64,
# AND 65+, 2018
#  DOMAIN WITH_AN_EXPENSE('Any Expense') WITH_AN_EXPENSE('Any Expense')*AGELAST
print("\nMEAN AND MEDIAN EXPENSE PER PERSON WITH AN EXPENSE, OVERALL and FOR AGES 0-64, AND 65+, 2018")
fmt["AGELAST"] = apply_format(fmt["AGELAST"], AGECAT)
for dom in (["WITH_AN_EXPENSE"], ["WITH_AN_EXPENSE", "AGELAST"]):
    m = svy_mean(fmt, "TOTEXP18", domain=dom, **design)
    s = svy_total(fmt, "TOTEXP18", domain=dom, **design)
    q = svy_quantile(fmt, "TOTEXP18", quantiles=[0.5], domain=dom, **design)
    res = m.merge(s, on=dom + ["variable", "n"], suffixes=("_mean", "_sum")).merge(
        q[dom + ["estimate", "se"]].rename(columns={"estimate": "median", "se": "se_median"}), on=dom
    )
    print(res[res["WITH_AN_EXPENSE"] == "Any Expense"].to_string(index=False))
