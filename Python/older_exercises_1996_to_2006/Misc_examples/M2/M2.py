"""Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M2/M2.sas

/

PROGRAM:        C://MEPS/PROG/M2.SAS

DESCRIPTION:     THIS EXAMPLE SHOWS THE NEED FOR USING THE
                     STRATUM AND PSU VARIABLES WHEN ANALYZING MEPS
                     DATA FOR NATIONAL ESTIMATES.  THAT IS, TAKING
                     THE MEPS COMPLEX DESIGN PROPERTIES INTO
                     ACCOUNT.

INPUT FILE:      <MEPS_DIR>/DATA/H97.SAS7BDAT (2005 FULL-YEAR DATA FILE)

/

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# 2005 consolidated full-year file (HC-097)
puf97 = read_meps("h97", ["TOTEXP05", "VARPSU", "VARSTR", "PERWT05F"])

# Total ignoring the design (no STRATUM/CLUSTER -> SRS variance) vs the complex design
srstot = surveymeans(puf97, "TOTEXP05", "PERWT05F", strata=None, cluster=None).iloc[0]
comptot = surveymeans(puf97, "TOTEXP05", "PERWT05F").iloc[0]
print(f"SRS    TOTAL IS ${srstot.Sum:,.2f}")
print(f"SRS SE TOTAL IS ${srstot.StdDev:,.2f}")
print(f"COMPLEX DESIGN TOTAL    IS ${comptot.Sum:,.2f}")
print(f"COMPLEX DESIGN TOTAL SE IS ${comptot.StdDev:,.2f}")
