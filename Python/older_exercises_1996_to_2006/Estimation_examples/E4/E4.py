"""Python port of SAS/older_exercises_1996_to_2006/Estimation_examples/E4/E4.sas

/

PROGRAM:       <MEPS_DIR>/PROG/EXAMPLE_E4.SAS

DESCRIPTION:      THIS EXAMPLE SHOWS HOW TO COMPUTE FAMILY-LEVEL
               ESTIMATES, USING THE MEPS DEFINITION OF FAMILY
               RATHER THAN THE CPS DEFINITION.

               SEE SECTION 3.3 OF THE DOCUMENTATION FOR HC-060
               (THE 2001 MEPS FILL-YEAR FILE).

               THIS PROGRAM GENERATES THE FOLLOWING FAMILY-LEVEL
               ESTIMATES:
               (1) MEAN NUMBER OF PERSONS PER FAMILY.
               (2) 2001 MEAN TOTAL HEALTHCARE EXPENSES PER
                   FAMILY.
               (3) 2001 MEAN TOTAL HEALTHCARE EXPENSES PER
                   FAMILY SIZE.

INPUT FILE:      (1) <MEPS_DIR>/DATA/H60.SAS7BDAT
                        -- 2001 MEPS FULL-YEAR FILE

/

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# Read HC-060; family ID = DUID (5 digits) || FAMIDYR -----------------------------------
H60 = read_meps("h60", ["DUID", "FAMIDYR", "DUPERSID", "FAMWT01F", "VARSTR01", "VARPSU01", "FAMRFPYR", "FAMSZEYR",
                        "TOTEXP01"])
H60["DUIDFAMY"] = H60.DUID.astype(int).map("{:05d}".format) + H60.FAMIDYR.astype(str).str.strip()
H60 = H60.sort_values("DUIDFAMY", kind="stable")

# Family-level file: FIRST./LAST. accumulation -> groupby; keep families with FAMWT01F > 0
H60["FAMTOT01"] = H60.groupby("DUIDFAMY").TOTEXP01.cumsum()  # FAMTOT01 + TOTEXP01 (retained sum)
FAM_H60 = H60.groupby("DUIDFAMY").tail(1)
FAM_H60 = FAM_H60[FAM_H60.FAMWT01F > 0].drop(columns="TOTEXP01")
print(freq_list(FAM_H60, ["FAMSZEYR"]))
chk = ["40001A", "40006A", "40007A", "40010A", "40011A"]
print(H60[H60.DUIDFAMY.isin(chk)][["DUIDFAMY", "DUPERSID", "TOTEXP01", "FAMTOT01"]])
print(FAM_H60[FAM_H60.DUIDFAMY.isin(chk)][["DUIDFAMY", "FAMTOT01"]])

design = dict(weight="FAMWT01F", strata="VARSTR01", cluster="VARPSU01")
ddf = design_df(FAM_H60, "VARSTR01", "VARPSU01", "FAMWT01F")


show("Family size and family total expenses", clm(surveymeans(FAM_H60, ["FAMSZEYR", "FAMTOT01"], **design), ddf)
     [["VarName", "N", "SumWgt", "Mean", "StdErr", "LowerCL", "UpperCL"]])

# DOMAIN FAMSZEYR (formatted 1, 2, 3, 4, 5+)
FAM_H60["FAMSZE_F"] = sas_format(FAM_H60.FAMSZEYR, {1: "1", 2: "2", 3: "3", 4: "4", slice(5, None): "5+"})
show("Family total expenses by family size", clm(surveymeans(FAM_H60, "FAMTOT01", domain="FAMSZE_F", **design), ddf)
     [["FAMSZE_F", "N", "SumWgt", "Mean", "StdErr", "LowerCL", "UpperCL"]])
