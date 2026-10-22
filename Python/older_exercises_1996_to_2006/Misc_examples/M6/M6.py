# --------------------------------------------------------------------------
# PROGRAM:       C:\MEPS\PROG\M6.SAS
#
# DESCRIPTION:   THIS EXAMPLE SHOWS THE USE OF THE DIABETES CARE SUPPLEMENT
#                (DCS) WEIGHT VARIABLE (DIABW05F) FOR GENERATING ESTIMATES
#                FOR ANALYSES USING QUESTIONS FROM THE DCS.
#                TWO DCS VARIABLES ARE USED:
#                (1) DSA1C53  (NUMBER OF TIMES TESTED FOR HEMOGLOBIN A1C)
#                (2) DSINSU53 (DIABETES TREATED WITH INSULIN INJECTIONS?)
#
# INPUT FILES:   C:\MEPS\DATA\H97.SAS7BDAT (2005 FULL-YEAR DATA FILE)
#
# Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M6/M6.sas
# --------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, load_meps, svy_freq  # noqa: E402

pd.set_option("display.width", 200)

# SAS ranges: 21-50 is listed before 50-HIGH, so 50 maps to '21-50 TIMES'
A1CF = [(-9, -9, "-9 NOT ASCERTAINED"), (-8, -8, "-8 DK"), (-1, -1, "-1 INAPPLICABLE"), (1, 20, "1-20 TIMES"),
        (21, 50, "21-50 TIMES"), (50, np.inf, "51 TIMES OR MORE")]  # fmt: skip
INSF = {-9: "-9 NOT ASCERTAINED", -8: "-8 DK", -1: "-1 INAPPLICABLE", 1: "1 YES", 0: "0  NO"}

FY2005 = load_meps("h97", columns=["DUPERSID", "DIABW05F", "VARSTR", "VARPSU", "DSA1C53", "DSINSU53"])
FY2005["DSA1C53"] = apply_format(FY2005["DSA1C53"], A1CF)
FY2005["DSINSU53"] = apply_format(FY2005["DSINSU53"], INSF)

# PROC SURVEYFREQ with the DCS weight; persons with DIABW05F = 0 are kept in
# the design but contribute no weight
for var in ("DSA1C53", "DSINSU53"):
    print(f"TABLE {var} (WEIGHT DIABW05F)")
    print(svy_freq(FY2005, var, weight="DIABW05F", strata="VARSTR", psu="VARPSU").to_string(index=False), "\n")
