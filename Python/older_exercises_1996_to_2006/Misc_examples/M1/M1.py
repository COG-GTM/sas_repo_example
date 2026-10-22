# --------------------------------------------------------------------
# PROGRAM:      C:\MEPS\PROG\M1.SAS
#
# DESCRIPTION:  THIS EXAMPLE SHOWS THE NEED FOR USING WEIGHT
#               VARIABLES WHEN ANALYZING MEPS DATA FOR
#               NATIONAL ESTIMATES.
#
# INPUT FILE:   C:\MEPS\DATA\H97.SAS7BDAT (2005 FULL-YEAR DATA FILE)
#
# Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M1/M1.sas
# --------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import load_meps  # noqa: E402

RACETHNB = {0: "TOTAL", 1: "HISPANIC", 2: "BLACK-NO OTH RACE/NOT HISPANIC", 3: "ASIAN-NO OTH RACE/NOT HISPANIC",
            4: "OTHER/NOT HISPANIC"}  # fmt: skip

# Read 2005 consolidated full year file
PUF97 = load_meps("h97", columns=["RACETHNX", "PERWT05F"])

# Unweighted and weighted PROC FREQ percentages, merged by RACETHNX
unwt = pd.crosstab(PUF97["RACETHNX"], columns="PERCENT", normalize="columns")["PERCENT"] * 100
wt = pd.crosstab(PUF97["RACETHNX"], columns="WTPCT", values=PUF97["PERWT05F"], aggfunc="sum", normalize="columns")["WTPCT"] * 100
pct = pd.DataFrame({"UNWEIGHTED PERCENTAGE": unwt, "WEIGHTED PERCENTAGE": wt}).round(2)
pct.index = pct.index.map(lambda v: RACETHNB.get(v, v))
print(pct.rename_axis("RACETHNX").to_string())
