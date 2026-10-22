# ----------------------------------------------------------------
# PROGRAM:     C:\MEPS\PROG\E7.SAS
#
# DESCRIPTION: THIS EXAMPLE SHOWS HOW TO COMPUTE SELECTED ESTIMATES
#              FROM MEPS STATISTICAL BRIEF #188, "SCREENING
#              COLONOSCOPY AMONG U.S. NONINSTITUTIONALIZED ADULT
#              POPULATION AGE 50 AND OLDER, 2005".
#
#              NOTE THAT THE STATISTICAL BRIEF (PUBLISHED NOVEMBER
#              2007) USED THE MEPS 2005 POPULATION CHARACTERISTICS
#              FILE (HC-90) SO THE ESTIMATES GENERATED IN THIS
#              PROGRAM, USING THE 2005 CONSOLIDATED DATA FILE (HC-97)
#              DIFFER SLIGHTLY.
#
#              ALSO NOTE THAT WHILE THE SUBPOPULATION OF INTEREST IS
#              ADULTS AGE 50 AND OLDER, THE ENTIRE 2005 MEPS POPULATION
#              MUST BE USED IN ANALYSES IN ORDER TO GENERATE ACCURATE
#              STANDARD ERRORS.  AN AGE CATEGORY VARIABLE IS USED IN
#              THE PROC SURVEYFREQ TABLE STATEMENT TO REQUEST ESTIMATES
#              AND STANDARD ERRORS FOR THE AGE GROUPS DESIRED.
#
# INPUT FILES: C:\MEPS\DATA\H97.SAS7BDAT (2005 FULL-YEAR DATA FILE)
#
# Python port of SAS/older_exercises_1996_to_2006/Estimation_examples/E7/E7.sas
# ----------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, load_meps, svy_freq  # noqa: E402

pd.set_option("display.width", 200)

# Formats (PROC FORMAT) -------------------------------------------------------
AGECAT = {1: "1 AGE CATEGORY 0-49", 2: "2 AGE CATEGORY 50-64", 3: "3 AGE CATEGORY 65+"}
AGE50P = {1: "1 AGE CATEGORY 0-49", 2: "2 AGE CATEGORY 50+"}
YESNO = [(-np.inf, -1e-9, "<0"), (0, 0, "0"), (1, 1, "1, YES"), (2, 2, "2, NO")]
EDUCF = {-7: "-7 REFUSED", -8: "-8 DK", -9: "-9 NOT ASCERTAINED", -1: "-1 INAPPLICABLE", 1: "1 LESS THAN HIGH SCHOOL",
         2: "2 HIGH SCHOOL GRAD", 3: "3 AT LEAST SOME COLLEGE", 4: "4 UNKNOWN (-7/-8/-9)", 5: "5 YOUNGER THAN 16"}  # fmt: skip
RACEF = {-7: "-7 REFUSED", -8: "-8 DK", -9: "-9 NOT ASCERTAINED", -1: "-1 INAPPLICABLE", 1: "1 HISPANIC",
         2: "2 WHITE NON-HISPANIC", 3: "3 BLACK NON-HISPANIC", 4: "4 ASIAN NON-HISPANIC", 5: "5 OTHER NON-HISPANIC"}  # fmt: skip

# 2005 FY consolidated data file ----------------------------------------------
P = load_meps("h97", columns=["DUPERSID", "VARSTR", "VARPSU", "AGE05X", "AGE53X", "AGE42X", "AGE31X", "RACEX",
                              "HISPANX", "EDUCYR", "BOWEL53", "WHNBWL53", "PERWT05F"])  # fmt: skip
P["BOWELYES"] = np.where(P["BOWEL53"] == 1, 1, 0)

P["AGE"] = np.select([P["AGE05X"] >= 0, P["AGE53X"] >= 0, P["AGE42X"] >= 0, P["AGE31X"] >= 0],
                     [P["AGE05X"], P["AGE53X"], P["AGE42X"], P["AGE31X"]], default=-1)  # fmt: skip
# Age category - 3 levels and 2 levels
P["AGECAT"] = np.select([P["AGE"].between(0, 49), P["AGE"].between(50, 64), P["AGE"] >= 65], [1, 2, 3], default=-1)
P["AGE50PLUS"] = np.select([P["AGE"].between(0, 49), P["AGE"] >= 50], [1, 2], default=-1)

# RACETH: 1 Hispanic; non-Hispanic 2 White, 3 Black, 4 Amer. Ind/AK Native,
# 5 Asian, 6 Hawaiian/Pacific Isl., 7 Multiple races
P["RACETH"] = np.where(P["HISPANX"] == 1, 1, np.where(P["HISPANX"] == 2, P["RACEX"].map(dict(zip(range(1, 7), range(2, 8)))), np.nan))
# NEWRACE: 1 Hispanic, 2 White, 3 Black, 4 Asian, 5 AIAN/NHPI/multiple (non-Hispanic)
P["NEWRACE"] = P["RACETH"].map({1: 1, 2: 2, 3: 3, 5: 4, 4: 5, 6: 5, 7: 5})

# HIGHEDUC
P["HIGHEDUC"] = np.select(
    [P["AGE"] < 16, P["EDUCYR"] < 0, P["EDUCYR"] < 12, P["EDUCYR"] == 12, P["EDUCYR"] > 12], [5, 4, 1, 2, 3], default=-1
)

P["BOWEL53"] = apply_format(P["BOWEL53"], YESNO)
P["AGECAT"] = apply_format(P["AGECAT"], AGECAT)
P["AGE50PLUS"] = apply_format(P["AGE50PLUS"], AGE50P)
P["NEWRACE"] = apply_format(P["NEWRACE"], RACEF)
P["HIGHEDUC"] = apply_format(P["HIGHEDUC"], EDUCF)

design = dict(weight="PERWT05F", strata="VARSTR", psu="VARPSU")


def row_table(rows):
    """PROC SURVEYFREQ; TABLE rows*BOWEL53 / ROW  (row percents = domain percents)."""
    res = svy_freq(P, "BOWEL53", domain=rows, **design)
    print(f"TABLE {'*'.join(rows)}*BOWEL53 / ROW")
    print(res.to_string(index=False), "\n")


row_table(["AGECAT"])
row_table(["AGE50PLUS"])
row_table(["AGE50PLUS", "NEWRACE"])
row_table(["AGE50PLUS", "HIGHEDUC"])
