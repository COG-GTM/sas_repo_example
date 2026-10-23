"""Python port of SAS/older_exercises_1996_to_2006/Estimation_examples/E5/E5.sas

/

PROGRAM:       <MEPS_DIR>/PROG/EXAMPLE_E5.SAS

DESCRIPTION:      THIS EXAMPLE SHOWS HOW TO COMPUTE EVENT-LEVEL
               ESTIMATES.
               IN THIS PROGRAM
               (1) THE 2001 MEPS INPATIENT HOSPITAL STAYS FILE IS
                   USED TO COMPUTE MEAN FACILITY EXPENSE PER
                   STAY.
               (2) THE 2001 MEPS OFFICE-BASED MEDICAL PROVIDER
                   VISITS FILE IS USED TO COMPUTE MEAN
                   EXPENSE PER OFFICE VISIT TO A MEDICAL
                   PROVIDER.

INPUT FILE:      (1) <MEPS_DIR>/DATA/H59D.SAS7BDAT
                        -- 2001 HOSPITAL STAYS FILE
               (2) <MEPS_DIR>/DATA/H59G.SAS7BDAT
                        -- 2001 OFFICE-BASED VISITS FILE

/

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

COLS = ["VarName", "N", "SumWgt", "Mean", "StdErr", "LowerCL", "UpperCL"]

# Event-level files: 2001 hospital inpatient stays (HC-059D), office-based visits (HC-059G)
for f, v in [("h59d", "IPFXP01X"), ("h59g", "OBXP01X")]:
    ev = read_meps(f, ["DUPERSID", v, "PERWT01F", "VARSTR01", "VARPSU01"])
    ddf = design_df(ev, "VARSTR01", "VARPSU01", "PERWT01F")
    show(f"{f.upper()}: mean {v} per event", clm(surveymeans(ev, v, "PERWT01F", "VARSTR01", "VARPSU01"), ddf)[COLS])
