"""Python port of SAS/summary_tables_examples/care_access_2017.sas

Example code to replicate estimates from the MEPS-HC Data Tools summary tables

Accessibility and quality of care: Access to Care, 2017

Reasons for difficulty receiving needed care
 - Number/percent of people
 - By poverty status

Input file: <MEPS_DIR>/h201.sas7bdat (2017 full-year consolidated)

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path


sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# Load FYC file (SAS: DATA h201; SET "C:/MEPS/h201.sas7bdat") ------------------
h201 = read_meps("h201")

# Define variables (SAS DATA step) ----------------------------------------------
MEPS = h201.copy()


def flag(cond):
    return cond.astype(int)


# Reasons for difficulty receiving needed care
#  any delay / unable to receive needed care
MEPS["delay_MD"] = flag((MEPS.MDUNAB42 == 1) | (MEPS.MDDLAY42 == 1))
MEPS["delay_DN"] = flag((MEPS.DNUNAB42 == 1) | (MEPS.DNDLAY42 == 1))
MEPS["delay_PM"] = flag((MEPS.PMUNAB42 == 1) | (MEPS.PMDLAY42 == 1))
# Among people unable or delayed, how many...
#  ...couldn't afford
MEPS["afford_MD"] = flag((MEPS.MDDLRS42 == 1) | (MEPS.MDUNRS42 == 1))
MEPS["afford_DN"] = flag((MEPS.DNDLRS42 == 1) | (MEPS.DNUNRS42 == 1))
MEPS["afford_PM"] = flag((MEPS.PMDLRS42 == 1) | (MEPS.PMUNRS42 == 1))
#  ...had insurance problems
MEPS["insure_MD"] = flag(MEPS.MDDLRS42.isin([2, 3]) | MEPS.MDUNRS42.isin([2, 3]))
MEPS["insure_DN"] = flag(MEPS.DNDLRS42.isin([2, 3]) | MEPS.DNUNRS42.isin([2, 3]))
MEPS["insure_PM"] = flag(MEPS.PMDLRS42.isin([2, 3]) | MEPS.PMUNRS42.isin([2, 3]))
#  ...other
MEPS["other_MD"] = flag((MEPS.MDDLRS42 > 3) | (MEPS.MDUNRS42 > 3))
MEPS["other_DN"] = flag((MEPS.DNDLRS42 > 3) | (MEPS.DNUNRS42 > 3))
MEPS["other_PM"] = flag((MEPS.PMDLRS42 > 3) | (MEPS.PMUNRS42 > 3))

for r in ["delay", "afford", "insure", "other"]:
    MEPS[f"{r}_ANY"] = MEPS[[f"{r}_MD", f"{r}_DN", f"{r}_PM"]].max(axis=1)

# Define domain and adjust weights so observations are not dropped
#  - domain includes persons eligible to receive the 'access to care'
#    supplement and who experienced difficulty receiving needed care
MEPS["domain"] = flag((MEPS.ACCELI42 == 1) & (MEPS.delay_ANY == 1))
MEPS.loc[(MEPS.domain == 0) & (MEPS.PERWT17F == 0), "PERWT17F"] = 1

# PROC FORMAT
poverty = {1: "1 Negative or poor", 2: "2 Near-poor", 3: "3 Low income", 4: "4 Middle Income", 5: "5 High Income"}
MEPS["POVCAT17"] = sas_format(MEPS.POVCAT17, poverty)

# QC new variables (PROC FREQ / LIST)
print(freq_list(MEPS, ["delay_MD", "MDUNAB42", "MDDLAY42"]))
print(freq_list(MEPS, ["delay_DN", "DNUNAB42", "DNDLAY42"]))
print(freq_list(MEPS, ["delay_PM", "PMUNAB42", "PMDLAY42"]))
print(freq_list(MEPS, ["delay_ANY", "delay_MD", "delay_DN", "delay_PM"]))

# Calculate estimates using survey procedures ------------------------------------
# Reasons for difficulty receiving any needed care, by poverty status
#  Sum  = Number of people
#  Mean = Percent of people
est = surveymeans(MEPS, ["afford_ANY", "insure_ANY", "other_ANY"], weight="PERWT17F",
                  strata="VARSTR", cluster="VARPSU", domain=["domain", "POVCAT17"], missing=True)
show("Reasons for difficulty receiving needed care, by poverty status (domain = 1)",
     est[est.domain == 1][["POVCAT17", "VarName", "N", "Sum", "StdDev", "Mean", "StdErr"]])
