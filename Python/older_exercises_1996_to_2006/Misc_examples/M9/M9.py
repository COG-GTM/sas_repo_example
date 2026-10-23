"""Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M9/M9.sas

/

PROGRAM:        C://MEPS/PROG/M9.SAS

DESCRIPTION:   THIS EXAMPLE USES THE 2005 FULL-YEAR FILE TO OUTPUT
               DESCRIPTIVE STATISTICS SHOWING HEALTH INSURANCE
               STATUS AND HEALTHCARE UTILIZATION.

               TYPES OF EVENTS USED IN THIS PROGRAM:

               PRESCRIBED MEDICINE USE (RXTOT05)
               OFFICE-BASED VISITS (OBTOT05)
               EMERGENCY DEPT. VISITS (ERTOT05)

INPUT FILE:       <MEPS_DIR>/DATA/H97.SAS7BDAT (2005 FULL-YEAR DATA FILE)

/

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

YESNO = {-9: "-9 NOT ASCERTAINED", -8: "-8 DK", -7: "-7 REFUSED", -1: "-1 INAPPLICABLE", 1: "UNINSURED",
         2: "INSURED"}
INSCOV = {1: "ANY PRIVATE", 2: "PUBLIC ONLY", 3: "UNINSURED"}

# 2005 consolidated full-year file (HC-097)
puf97 = read_meps("h97", ["UNINS05", "INSCOV05", "RXTOT05", "OBTOTV05", "ERTOT05", "VARPSU", "VARSTR", "PERWT05F"])
puf97["UNINS05"] = sas_format(puf97.UNINS05, YESNO)
puf97["INSCOV05"] = sas_format(puf97.INSCOV05, INSCOV)
for v in ["RXTOT05", "OBTOTV05", "ERTOT05"]:
    show(f"{v} (overall)", surveymeans(puf97, v, "PERWT05F"))
    for dom in ["UNINS05", "INSCOV05"]:  # DOMAIN UNINS05 INSCOV05 -> one domain analysis each
        show(f"{v} by {dom}", surveymeans(puf97, v, "PERWT05F", domain=dom))
