"""Python port of SAS/summary_tables_examples/care_quality_2016.sas

Example code to replicate estimates from the MEPS-HC Data Tools summary tables

Accessibility and quality of care: Quality of Care, 2016

Self-administered questionnaire (SAQ):
 - Number/percent of adults by ability to schedule a routine appointment
 - By insurance coverage status

Input file: <MEPS_DIR>/h192.ssp (2016 full-year consolidated)

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# Load FYC file (SAS: PROC XCOPY) -----------------------------------------------
h192 = read_meps("h192")

# Define variables ----------------------------------------------------------------
#  - For 1996-2011, create INSURC from INSCOV and 'EV' variables (see ins_age_2016.py)
MEPS = h192.copy()
# Domain = adults who made an appointment for routine care; adjust zero weights
# outside the domain so observations are not dropped.
MEPS["domain"] = ((MEPS.ADRTCR42 == 1) & (MEPS.AGELAST >= 18)).astype(int)
MEPS.loc[(MEPS.domain == 0) & (MEPS.SAQWT16F == 0), "SAQWT16F"] = 1

# PROC FORMAT
freq = {4: "Always", 3: "Usually", (1, 2): "Sometimes/Never", (-7, -8, -9): "Don't know/Non-response",
        -1: "Inapplicable"}
insurance = {1: "<65, Any private", 2: "<65, Public only", 3: "<65, Uninsured", 4: "65+, Medicare only",
             5: "65+, Medicare and private", 6: "65+, Medicare and other public", (7, 8): "65+, No medicare"}
MEPS["ADRTWW42"] = sas_format(MEPS.ADRTWW42, freq, missing="Missing")
MEPS["INSURC16"] = sas_format(MEPS.INSURC16, insurance)

# Calculate estimates using survey procedures (PROC SURVEYFREQ ... / ROW) ---------
out = surveyfreq(MEPS, ["domain", "INSURC16", "ADRTWW42"], weight="SAQWT16F", strata="VARSTR", cluster="VARPSU",
                 missing=True)

# Adults with an appointment: how often got care as soon as wanted, by insurance
show("Ability to schedule a routine appointment, by insurance coverage (domain = 1)",
     out[out.domain == 1][["ADRTWW42", "INSURC16", "Frequency", "WgtFreq", "StdDev", "RowPercent", "RowStdErr"]])
