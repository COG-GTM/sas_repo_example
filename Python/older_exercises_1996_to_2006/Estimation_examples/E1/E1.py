"""Python port of SAS/older_exercises_1996_to_2006/Estimation_examples/E1/E1.sas

/

PROGRAM:       <MEPS_DIR>/PROG/EXAMPLE_E1.SAS

DESCRIPTION:      THIS EXAMPLE SHOWS HOW TO COMPUTE PERSON-LEVEL
               ESTIMATES FOR PERSON-LEVEL HEALTHCARE EXPENDITURES.
               ESTIMATES INCLUDE: MEANS, PROPORTIONS, TOTALS.

INPUT FILE:      <MEPS_DIR>/DATA/H60.SAS7BDAT (2001 FULL-YEAR DATA FILE)
                     -- PERSON-LEVEL FILE

/

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403



# Read HC-060 (2001 full-year file); AGE = AGE01X, else AGE42X, else AGE31X ---------
H60 = read_meps("h60", ["TOTEXP01", "SEX", "AGE01X", "AGE42X", "AGE31X", "PERWT01F", "VARPSU01", "VARSTR01"])
H60["AGE"] = np.select([H60.AGE01X >= 0, H60.AGE42X >= 0, H60.AGE31X >= 0], [H60.AGE01X, H60.AGE42X, H60.AGE31X],
                       H60.AGE01X)
H60["ANY_EXP"] = np.where(H60.TOTEXP01 == 0, 0, 100)  # 100 so the mean reads as a percentage

# PROC FORMAT
AGEF = {slice(0, 64): "0-64", slice(65, 90): "65-90"}
SEXF = {1: "MALE", 2: "FEMALE"}
H60["AGE_F"] = sas_format(H60.AGE, AGEF)
H60["SEX_F"] = sas_format(H60.SEX, SEXF)
design = dict(weight="PERWT01F", strata="VARSTR01", cluster="VARPSU01")
ddf = design_df(H60, "VARSTR01", "VARPSU01", "PERWT01F")

# MEANS: overall and DOMAIN AGE SEX -------------------------------------------------
for v in ["TOTEXP01", "ANY_EXP"]:
    show(f"{v}: overall", clm(surveymeans(H60, v, **design), ddf)[["VarName", "N", "SumWgt", "Mean", "StdErr", "CV", "LowerCL", "UpperCL"]])
    for dom in ["AGE_F", "SEX_F"]:
        show(f"{v}: DOMAIN {dom[:-2]}",
             clm(surveymeans(H60, v, domain=dom, **design), ddf)[[dom, "N", "SumWgt", "Mean", "StdErr", "CV", "LowerCL", "UpperCL"]])

# PROC SURVEYREG: TOTEXP01 = SEX, with SEX recoded FEMALE = 0, MALE = 1 -------------
H60V2 = H60.assign(SEX=np.where(H60.SEX == 2, 0, H60.SEX))
show("PROC SURVEYREG: TOTEXP01 = SEX", surveyreg(H60V2, "TOTEXP01 ~ SEX", family="linear", **design))

# TOTALS: SUM and SUMWGT --------------------------------------------------------------
tot = surveymeans(H60, "TOTEXP01", **design)
show("TOTEXP01: total", clm(tot, ddf, "Sum", "StdDev")[["VarName", "N", "SumWgt", "Sum", "StdDev", "LowerCL", "UpperCL"]])
