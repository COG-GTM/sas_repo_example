"""Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M6/M6.sas

/

PROGRAM:       C://MEPS/PROG/M6.SAS

DESCRIPTION:   THIS EXAMPLE SHOWS THE USE OF THE DIABETES CARE SUPPLEMENT
               (DCS) WEIGHT VARIABLE (DIABW05F) FOR GENERATING ESTIMATES
               FOR ANALYSES USING QUESTIONS FROM THE DCS.

               TWO DCS VARIABLES ARE USED:

               (1) DSA1C53  (NUMBER OF TIMES TESTED FOR HEMOGLOBIN A1C)
               (2) DSINSU53 (DIABETES TREATED WITH INSULIN INJECTIONS?)

INPUT FILES:      <MEPS_DIR>/DATA/H97.SAS7BDAT (2005 FULL-YEAR DATA FILE)

/

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path


sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

A1CF = {-9: "-9 NOT ASCERTAINED", -8: "-8 DK", -1: "-1 INAPPLICABLE", slice(1, 20): "1-20 TIMES",
        slice(21, 50): "21-50 TIMES", slice(50, None): "51 TIMES OR MORE"}
INSF = {-9: "-9 NOT ASCERTAINED", -8: "-8 DK", -1: "-1 INAPPLICABLE", 1: "1 YES", 0: "0  NO"}

# 2005 consolidated full-year file (HC-097), diabetes care survey variables
fy2005 = read_meps("h97", ["DUPERSID", "DIABW05F", "VARSTR", "VARPSU", "DSA1C53", "DSINSU53"])
fy2005["DSA1C53"] = sas_format(fy2005.DSA1C53, A1CF)
fy2005["DSINSU53"] = sas_format(fy2005.DSINSU53, INSF)
# PROC SURVEYFREQ uses only records with a positive weight (DIABW05F > 0 for diabetics)
for v in ["DSA1C53", "DSINSU53"]:
    show(f"Table of {v}", surveyfreq(fy2005, [v], "DIABW05F"), "{:,.4f}")
