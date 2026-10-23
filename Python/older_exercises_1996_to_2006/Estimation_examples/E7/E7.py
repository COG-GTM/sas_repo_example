"""Python port of SAS/older_exercises_1996_to_2006/Estimation_examples/E7/E7.sas

/

PROGRAM:     <MEPS_DIR>/PROG/E7.SAS

DESCRIPTION: THIS EXAMPLE SHOWS HOW TO COMPUTE SELECTED ESTIMATES
                         FROM MEPS STATISTICAL BRIEF #188, "SCREENING
                         COLONOSCOPY AMONG U.S. NONINSTITUTIONALIZED ADULT
                         POPULATION AGE 50 AND OLDER, 2005".

                         NOTE THAT THE STATISTICAL BRIEF (PUBLISHED NOVEMBER
                         2007) USED THE MEPS 2005 POPULATION CHARACTERISTICS
                         FILE (HC-90) SO THE ESTIMATES GENERATED IN THIS
                         PROGRAM, USING THE 2005 CONSOLIDATED DATA FILE (HC-97)
                         DIFFER SLIGHTLY.

                     ALSO NOTE THAT WHILE THE SUBPOPULATION OF INTEREST IS
                     ADULTS AGE 50 AND OLDER, THE ENTIRE 2005 MEPS POPULATION
                     MUST BE USED IN ANALYSES IN ORDER TO GENERATE ACCURATE
                     STANDARD ERRORS.  AN AGE CATEGORY VARIABLE IS USED IN
                     THE PROC SURVEYFREQ TABLE STATEMENT TO REQUEST ESTIMATES
                     AND STANDARD ERRORS FOR THE AGE GROUPS DESIRED.


INPUT FILES: <MEPS_DIR>/DATA/H97.SAS7BDAT (2005 FULL-YEAR DATA FILE)

/

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# PROC FORMAT
YESNO = {slice(None, -0.5): "<0", 0: "0", 1: "1, YES", 2: "2, NO"}
AGECAT_F = {1: "1 AGE CATEGORY 0-49", 2: "2 AGE CATEGORY 50-64", 3: "3 AGE CATEGORY 65+"}
AGE50P = {1: "1 AGE CATEGORY 0-49", 2: "2 AGE CATEGORY 50+"}
EDUCF = {-7: "-7 REFUSED", -8: "-8 DK", -9: "-9 NOT ASCERTAINED", -1: "-1 INAPPLICABLE", 1: "1 LESS THAN HIGH SCHOOL",
         2: "2 HIGH SCHOOL GRAD", 3: "3 AT LEAST SOME COLLEGE", 4: "4 UNKNOWN (-7/-8/-9)", 5: "5 YOUNGER THAN 16"}
RACEF = {-7: "-7 REFUSED", -8: "-8 DK", -9: "-9 NOT ASCERTAINED", -1: "-1 INAPPLICABLE", 1: "1 HISPANIC",
         2: "2 WHITE NON-HISPANIC", 3: "3 BLACK NON-HISPANIC", 4: "4 ASIAN NON-HISPANIC", 5: "5 OTHER NON-HISPANIC"}

# 2005 full-year consolidated file (HC-097) --------------------------------------------
PUF97 = read_meps("h97", ["DUPERSID", "VARSTR", "VARPSU", "AGE05X", "AGE53X", "AGE42X", "AGE31X", "RACEX", "HISPANX",
                          "EDUCYR", "BOWEL53", "WHNBWL53", "PERWT05F"])
PUF97["BOWELYES"] = np.where(PUF97.BOWEL53 == 1, 1, 0)
PUF97["AGE"] = np.select([PUF97.AGE05X >= 0, PUF97.AGE53X >= 0, PUF97.AGE42X >= 0, PUF97.AGE31X >= 0],
                         [PUF97.AGE05X, PUF97.AGE53X, PUF97.AGE42X, PUF97.AGE31X], -1)
age = PUF97.AGE
PUF97["AGECAT"] = np.select([age.between(0, 49), age.between(50, 64), age >= 65], [1, 2, 3], -1)
PUF97["AGE50PLUS"] = np.select([age.between(0, 49), age >= 50], [1, 2], -1)
# RACETH (left missing when HISPANX is not 1/2, as in the SAS DATA step)
hisp = PUF97.HISPANX == 1
nonhisp = PUF97.HISPANX == 2
PUF97["RACETH"] = np.select([hisp] + [nonhisp & (PUF97.RACEX == r) for r in range(1, 7)], [1, 2, 3, 4, 5, 6, 7], np.nan)
PUF97["NEWRACE"] = PUF97.RACETH.map({1: 1, 2: 2, 3: 3, 5: 4, 4: 5, 6: 5, 7: 5})
PUF97["HIGHEDUC"] = np.select([age < 16, PUF97.EDUCYR < 0, PUF97.EDUCYR < 12, PUF97.EDUCYR == 12, PUF97.EDUCYR > 12],
                              [5, 4, 1, 2, 3], -1)

PUF97["BOWEL53_F"] = sas_format(PUF97.BOWEL53, YESNO)
PUF97["AGECAT_F"] = sas_format(PUF97.AGECAT, AGECAT_F)
PUF97["AGE50PLUS_F"] = sas_format(PUF97.AGE50PLUS, AGE50P)
PUF97["NEWRACE_F"] = sas_format(PUF97.NEWRACE, RACEF)
PUF97["HIGHEDUC_F"] = sas_format(PUF97.HIGHEDUC, EDUCF)

design = dict(weight="PERWT05F", strata="VARSTR", cluster="VARPSU")
COLS = ["Frequency", "WgtFreq", "StdDev", "Percent", "StdErr", "RowPercent", "RowStdErr"]
for tab in [["AGECAT_F", "BOWEL53_F"], ["AGE50PLUS_F", "BOWEL53_F"], ["AGE50PLUS_F", "NEWRACE_F", "BOWEL53_F"],
            ["AGE50PLUS_F", "HIGHEDUC_F", "BOWEL53_F"]]:
    show("Table of " + " by ".join(t[:-2] for t in tab), surveyfreq(PUF97, tab, **design)[tab + COLS], "{:,.4f}")
