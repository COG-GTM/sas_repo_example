# -----------------------------------------------------------------------------
# Example code to replicate estimates from the MEPS-HC Data Tools summary tables
#
# Accessibility and quality of care: Quality of Care, 2016
#
# Self-administered questionnaire (SAQ):
#  - Number/percent of adults by ability to schedule a routine appointment
#  - By insurance coverage status
#
# Input file: C:/MEPS/h192.ssp (2016 full-year consolidated)
#
# Python port of SAS/summary_tables_examples/care_quality_2016.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import MEPS_DIR, add_domain, apply_format, read_meps, svy_freq  # noqa: E402

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)

# Load FYC file (SAS transport file) ------------------------------------------

h192 = read_meps(f"{MEPS_DIR}/h192.ssp")

# Define variables ------------------------------------------------------------
#  - For 1996-2011, create INSURC from INSCOV and 'EV' variables
#     (for 1996, use 'EVER' vars)

MEPS = h192.copy()

# Define domain and adjust weights so observations are not dropped
#  - domain includes adults who made an appointment
MEPS = add_domain(MEPS, (MEPS["ADRTCR42"] == 1) & (MEPS["AGELAST"] >= 18), weight="SAQWT16F")

# Formats (PROC FORMAT) -------------------------------------------------------

freq = {
    4: "Always",
    3: "Usually",
    1: "Sometimes/Never",
    2: "Sometimes/Never",
    -7: "Don't know/Non-response",
    -8: "Don't know/Non-response",
    -9: "Don't know/Non-response",
    -1: "Inapplicable",
}
insurance = {
    1: "<65, Any private",
    2: "<65, Public only",
    3: "<65, Uninsured",
    4: "65+, Medicare only",
    5: "65+, Medicare and private",
    6: "65+, Medicare and other public",
    7: "65+, No medicare",
    8: "65+, No medicare",
}
MEPS["ADRTWW42"] = apply_format(MEPS["ADRTWW42"], freq)
MEPS["INSURC16"] = apply_format(MEPS["INSURC16"], insurance)

# Calculate estimates using survey procedures ---------------------------------
#  - use SAQWT16F weight variable, since outcome variable comes from SAQ
#  - TABLES domain*INSURC16*ADRTWW42 / row

out = svy_freq(
    MEPS, "ADRTWW42", weight="SAQWT16F", strata="VARSTR", psu="VARPSU", domain=["domain", "INSURC16"]
)

# Ability to schedule a routine appointment (adults), by insurance coverage
print("Ability to schedule a routine appointment (adults), by insurance coverage")
keep = (out["domain"] == 1) & (out["ADRTWW42"] != "Missing") & (out["INSURC16"] != "Missing")
print(out[keep].drop(columns="domain").to_string(index=False))
