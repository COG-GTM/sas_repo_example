"""Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M1/M1.sas

/

PROGRAM:         <MEPS_DIR>/PROG/M1.SAS

DESCRIPTION:     THIS EXAMPLE SHOWS THE NEED FOR USING WEIGHT
                     VARIABLES WHEN ANALYZING MEPS DATA FOR
                     NATIONAL ESTIMATES.


INPUT FILE:      <MEPS_DIR>/DATA/H97.SAS7BDAT (2005 FULL-YEAR DATA FILE)

/

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

RACETHNB = {0: "TOTAL", 1: "HISPANIC", 2: "BLACK-NO OTH RACE/NOT HISPANIC", 3: "ASIAN-NO OTH RACE/NOT HISPANIC",
            4: "OTHER/NOT HISPANIC"}

# 2005 consolidated full-year file (HC-097)
puf97 = read_meps("h97", ["RACETHNX", "PERWT05F"])
r = sas_format(puf97.RACETHNX, RACETHNB)
# PROC FREQ ... OUT= unweighted and WEIGHT PERWT05F weighted percentages, then MERGE BY RACETHNX
unwt = (100 * r.value_counts(normalize=True)).rename("UNWEIGHTED PERCENTAGE")
wt = (100 * puf97.PERWT05F.groupby(r).sum() / puf97.PERWT05F.sum()).rename("WEIGHTED PERCENTAGE")
pct = pd.concat([unwt, wt], axis=1).sort_index().rename_axis("RACETHNX").reset_index()
show("Unweighted vs weighted percentages by race/ethnicity", pct, "{:5.2f}")
