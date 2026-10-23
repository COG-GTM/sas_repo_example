"""Python port of SAS/summary_tables_examples/ins_age_2016.sas

Example code to replicate estimates from the MEPS-HC Data Tools summary tables

Health insurance, 2016:
 - Number/percent of people
 - By insurance coverage and age groups

Input file: <MEPS_DIR>/h192.ssp (2016 full-year consolidated)

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path


sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# Load FYC file (SAS: PROC XCOPY IN=h192 OUT=WORK IMPORT) -----------------------
h192 = read_meps("h192")  # pandas.read_sas(<MEPS_DIR>/h192.ssp, format="xport")

# Define variables ----------------------------------------------------------------
#  - For 1996-2007, AGELAST must be created from AGEyyX, AGE42X, AGE31X
#  - For 1996-2011, create INSURC from INSCOV and 'EV' variables
#     (for 1996, use 'EVER' vars):
#
#     public   = (MCDEV16 == 1) | (OPAEV16 == 1) | (OPBEV16 == 1)
#     medicare = (MCREV16 == 1)
#     private  = (INSCOV16 == 1)
#
#     mcr_priv = medicare &  private
#     mcr_pub  = medicare & ~private &  public
#     mcr_only = medicare & ~private & ~public
#     no_mcr   = ~medicare
#
#     ins_gt65 = 4*mcr_only + 5*mcr_priv + 6*mcr_pub + 7*no_mcr
#     INSURC16 = np.where(AGELAST < 65, INSCOV16, ins_gt65)

# PROC FORMAT
agegrps = {slice(None, 4): "Under 5", slice(5, 17): "5-17", slice(18, 44): "18-44",
           slice(45, 64): "45-64", slice(65, None): "65+"}
insurance = {1: "<65, Any private", 2: "<65, Public only", 3: "<65, Uninsured", 4: "65+, Medicare only",
             5: "65+, Medicare and private", 6: "65+, Medicare and other public", (7, 8): "65+, No medicare"}
h192["AGELAST"] = sas_format(h192.AGELAST, agegrps)
h192["INSURC16"] = sas_format(h192.INSURC16, insurance)

# Calculate estimates using survey procedures (PROC SURVEYFREQ ... / ROW) ---------
out = surveyfreq(h192, ["AGELAST", "INSURC16"], weight="PERWT16F", strata="VARSTR", cluster="VARPSU",
                 missing=True)

# Insurance coverage status by age groups
show("Insurance coverage status by age groups",
     out[["AGELAST", "INSURC16", "WgtFreq", "StdDev", "RowPercent", "RowStdErr"]])
