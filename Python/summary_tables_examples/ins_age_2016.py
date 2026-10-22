# -----------------------------------------------------------------------------
# Example code to replicate estimates from the MEPS-HC Data Tools summary tables
#
# Health insurance, 2016:
#  - Number/percent of people
#  - By insurance coverage and age groups
#
# Input file: C:\MEPS\h192.ssp (2016 full-year consolidated)
#
# Python port of SAS/summary_tables_examples/ins_age_2016.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import MEPS_DIR, apply_format, read_meps, svy_freq  # noqa: E402

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)

# Load FYC file (SAS transport file) ------------------------------------------

h192 = read_meps(f"{MEPS_DIR}/h192.ssp")

# Define variables ------------------------------------------------------------
#  - For 1996-2007, AGELAST must be created from AGEyyX, AGE42X, AGE31X
#  - For 1996-2011, create INSURC from INSCOV and 'EV' variables
#     (for 1996, use 'EVER' vars):
#
#     public   = (MCDEV16 = 1) or (OPAEV16=1) or (OPBEV16=1);
#     medicare = (MCREV16 = 1);
#     private  = (INSCOV16 = 1);
#
#     mcr_priv = (medicare and  private);
#     mcr_pub  = (medicare and ~private and public);
#     mcr_only = (medicare and ~private and ~public);
#     no_mcr   = (~medicare);
#
#     ins_gt65 = 4*mcr_only + 5*mcr_priv + 6*mcr_pub + 7*no_mcr;
#     if AGELAST < 65 then INSURC16 = INSCOV16;
#      else INSURC16 = ins_gt65;

# Formats (PROC FORMAT) -------------------------------------------------------

agegrps = [
    (-np.inf, 4, "Under 5"),
    (5, 17, "5-17"),
    (18, 44, "18-44"),
    (45, 64, "45-64"),
    (65, np.inf, "65+"),
]
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
h192["AGELAST"] = apply_format(h192["AGELAST"], agegrps)
h192["INSURC16"] = apply_format(h192["INSURC16"], insurance)

# Calculate estimates using survey procedures ---------------------------------
#  - TABLES AGELAST*INSURC16 / row

out = svy_freq(h192, "INSURC16", weight="PERWT16F", strata="VARSTR", psu="VARPSU", domain="AGELAST")

# Insurance coverage status by age groups
print("Insurance coverage status by age groups")
keep = (out["n"] > 0) & (out["AGELAST"] != "Missing") & (out["INSURC16"] != "Missing")
print(out[keep].to_string(index=False))
