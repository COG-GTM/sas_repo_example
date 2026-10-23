"""Python port of SAS/workshop_exercises/exercise_1c/Exercise1c.sas

This program generates the following estimates on national health care expenses
for the civilian noninstitutionized population, 2018:
  - Overall expenses (National totals)
  - Percentage of persons with an expense
  - Mean expense per person
  - Mean/median expense per person with an expense:
    - Mean expense per person with an expense
    - Mean expense per person with an expense, by age group
    - Median expense per person with an expense, by age group
 Input file:
 - 2018 Full-year consolidated file

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path


sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# Create user-defined formats (PROC FORMAT)
agecat = {slice(None, 64): "0-64", slice(65, None): "65+"}
totexp18_cate = {0: "No Expense"}  # Other = 'Any Expense'

# Load FYC file (SAS: SET CDATA.H209V9 (KEEP=...)) --------------------------------
PUF209 = read_meps("h209", ["TOTEXP18", "AGELAST", "VARSTR", "VARPSU", "PERWT18F", "PANEL"])
PUF209["WITH_AN_EXPENSE"] = sas_format(PUF209.TOTEXP18, totexp18_cate, other="Any Expense")
PUF209["CHAR_WITH_AN_EXPENSE"] = PUF209.WITH_AN_EXPENSE  # PUT(TOTEXP18, totexp18_cate.)
PUF209["AGECAT"] = sas_format(PUF209.AGELAST, agecat)

# PROC CONTENTS
print(PUF209.dtypes)

# SURVEYMEANS with a CLASS (categorical) variable: proportion and number in each level
cls = surveyfreq(PUF209, ["WITH_AN_EXPENSE"], weight="PERWT18F", strata="VARSTR", cluster="VARPSU")
cls["Mean"], cls["StdErr"] = cls.Percent / 100, cls.StdErr / 100
show("SURVEYMEANS, CLASS WITH_AN_EXPENSE", cls[["WITH_AN_EXPENSE", "Frequency", "Mean", "StdErr", "WgtFreq", "StdDev"]])

# SURVEYFREQ on the character version
show("SURVEYFREQ, TABLES CHAR_WITH_AN_EXPENSE",
     surveyfreq(PUF209, ["CHAR_WITH_AN_EXPENSE"], weight="PERWT18F", strata="VARSTR", cluster="VARPSU")
     [["CHAR_WITH_AN_EXPENSE", "Frequency", "WgtFreq", "StdDev", "Percent", "StdErr"]])

# Mean, total and median expense among persons with an expense, overall and by age
# (DOMAIN WITH_AN_EXPENSE('Any Expense') WITH_AN_EXPENSE('Any Expense')*AGELAST)
for dom in (["WITH_AN_EXPENSE"], ["WITH_AN_EXPENSE", "AGECAT"]):
    m = surveymeans(PUF209, "TOTEXP18", weight="PERWT18F", strata="VARSTR", cluster="VARPSU", domain=dom)
    q = surveyquantile(PUF209, "TOTEXP18", weight="PERWT18F", strata="VARSTR", cluster="VARPSU", domain=dom)
    m, q = m[m.WITH_AN_EXPENSE == "Any Expense"], q[q.WITH_AN_EXPENSE == "Any Expense"]
    show(f"Domain {'*'.join(dom)}: mean and total", m[dom + ["N", "Mean", "StdErr", "Sum", "StdDev"]])
    show(f"Domain {'*'.join(dom)}: median", q[dom + ["Estimate", "StdErr", "LowerCL", "UpperCL"]])
