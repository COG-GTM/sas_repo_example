"""Python port of SAS/summary_tables_examples/care_access_2019.sas

Example code to replicate estimates from the MEPS-HC Data Tools summary tables

Accessibility and quality of care: Access to Care, 2019

Did not receive treatment because couldn't afford it
 - Number/percent of people
 - By poverty status

Input file: <MEPS_DIR>/h216.sas7bdat (2019 full-year consolidated)

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# Load FYC file (SAS: DATA h216; SET "C:/MEPS/h216.sas7bdat") ------------------
h216 = read_meps("h216")

# Define variables (SAS DATA step) ----------------------------------------------
MEPS = h216.copy()
# Didn't receive care because couldn't afford it
MEPS["afford_MD"] = (MEPS.AFRDCA42 == 1).astype(int)  # medical care
MEPS["afford_DN"] = (MEPS.AFRDDN42 == 1).astype(int)  # dental care
MEPS["afford_PM"] = (MEPS.AFRDPM42 == 1).astype(int)  # prescribed medicines
MEPS["afford_ANY"] = MEPS[["afford_MD", "afford_DN", "afford_PM"]].max(axis=1)  # any care

# Define domain and adjust weights so observations are not dropped
#  - includes persons eligible to receive the 'access to care' supplement
MEPS["domain"] = (MEPS.ACCELI42 == 1).astype(int)
MEPS.loc[(MEPS.domain == 0) & (MEPS.PERWT19F == 0), "PERWT19F"] = 1

# PROC FORMAT
poverty = {1: "1 Negative or poor", 2: "2 Near-poor", 3: "3 Low income", 4: "4 Middle Income", 5: "5 High Income"}
MEPS["POVCAT19"] = sas_format(MEPS.POVCAT19, poverty)

# QC new variables (PROC FREQ / LIST)
print(freq_list(MEPS, ["AFRDCA42", "afford_MD"]))
print(freq_list(MEPS, ["AFRDDN42", "afford_DN"]))
print(freq_list(MEPS, ["AFRDPM42", "afford_PM"]))
print(freq_list(MEPS, ["afford_MD", "afford_DN", "afford_PM", "afford_ANY"]))

# Calculate estimates using survey procedures ------------------------------------
# Did not receive treatment because couldn't afford it, by poverty status
#  Sum  = Number of people
#  Mean = Percent of people
est = surveymeans(MEPS, ["afford_ANY", "afford_MD", "afford_DN", "afford_PM"], weight="PERWT19F",
                  strata="VARSTR", cluster="VARPSU", domain=["domain", "POVCAT19"], missing=True)
show("Did not receive treatment because couldn't afford it, by poverty status (domain = 1)",
     est[est.domain == 1][["POVCAT19", "VarName", "N", "Sum", "StdDev", "Mean", "StdErr"]])
