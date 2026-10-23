"""Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M3/M3.sas

/

PROGRAM:       <MEPS_DIR>/PROG/M3.SAS

DESCRIPTION:    THIS EXAMPLE SHOWS THE USE OF ID VARIABLES ON DIFFERENT
              MEPS PUBLIC-USE FILES (PUFs) AND HOW TO USE THESE ID
              VARIABLES TO MERGE MEPS FILES.

                     THIS EXAMPLE SHOWS PRINTS OF PERSON-LEVEL AND CONDITION-
                     LEVEL FILES TO SHOW THE USE OF THE ID VARIABLES.

                 DUPERSID (HC-097) IS A PERSON-LEVEL IDENTIFIER.
                 CONDIDX (HC-096) IS A PERSON-CONDITION-LEVEL IDENTIFIER.

              NOTE THAT THE MERGES USING DUPERSID ALSO USE PANEL, THAT IS,
              MERGE BY DUPERSID PANEL.  WHILE NOT NECESSARY WHEN PROCESSING
              ONLY ONE YEAR OF MEPS DATA, AS IN THIS EXAMPLE, IT IS GOOD
              PROGRAMMING PRACTICE TO ADD PANEL TO MERGES BECAUSE
              WHEN PROCESSING DATA ACROSS SEVERAL MEPS YEARS, DUPERSID (AND
              OTHER IDS) MAY NOT BE UNIQUE. SPECIFICALLY, STARTING WITH MEPS 2004,
              PANEL 9, IDS DUPLICATE EARLIER YEARS.

INPUT FILE:      (1) <MEPS_DIR>/DATA/H97.SAS7BDAT (2005 FULL-YEAR DATA FILE)
              (2) <MEPS_DIR>/DATA/H96.SAS7BDAT (2005 CONDITIONS FILE)

/

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

YESNO = {-9: "-9 NOT ASCERTAINED", -1: "-1 INAPPLICABLE", 1: "1 YES", 2: "2 NO"}
SEXF = {1: "1 MALE", 2: "2 FEMALE"}


def pr(df, first, last, cols):
    """PROC PRINT DATA=... (FIRSTOBS=first OBS=last)."""
    print(df[cols].iloc[first - 1:last].to_string(), "\n")


# 2005 consolidated full-year file (HC-097), sorted BY DUPERSID PANEL ------------------
fy2005 = read_meps("h97", ["DUID", "PID", "DUPERSID", "PANEL", "AGE05X", "SEX", "PERWT05F", "VARSTR", "VARPSU"])
fy2005 = fy2005.sort_values(["DUPERSID", "PANEL"], kind="stable").reset_index(drop=True)
fy2005.index += 1
pr(fy2005.assign(SEX=sas_format(fy2005.SEX, SEXF)), 40, 90, ["DUID", "PID", "DUPERSID", "PANEL", "AGE05X", "SEX"])

# 2005 conditions file (HC-096), sorted BY DUPERSID PANEL CONDIDX ---------------------
cond2005 = read_meps("h96", ["DUPERSID", "PANEL", "CONDN", "CONDIDX", "INJURY", "ICD9CODX"])
cond2005 = cond2005.sort_values(["DUPERSID", "PANEL", "CONDIDX"], kind="stable").reset_index(drop=True)
cond2005.index += 1
pr(cond2005.assign(INJURY=sas_format(cond2005.INJURY, YESNO)), 140, 190,
   ["DUPERSID", "PANEL", "CONDN", "CONDIDX", "INJURY", "ICD9CODX"])

COLS = ["DUPERSID", "PANEL", "CONDIDX", "AGE05X", "SEX", "INJURY", "ICD9CODX"]
# MERGE FY2005 COND2005 BY DUPERSID PANEL (no IN= subsetting -> outer join)
condinfo = fy2005.merge(cond2005, on=["DUPERSID", "PANEL"], how="outer", sort=True).reset_index(drop=True)
condinfo.index += 1
pr(condinfo.assign(SEX=sas_format(condinfo.SEX, SEXF), INJURY=sas_format(condinfo.INJURY, YESNO)), 140, 190, COLS)

# ... IF A AND B -> inner join: only matched records are kept
condinfo_b = fy2005.merge(cond2005, on=["DUPERSID", "PANEL"], how="inner", sort=True).reset_index(drop=True)
condinfo_b.index += 1
print("WHERE ONLY MATCHED RECORDS WERE KEPT")
pr(condinfo_b.assign(SEX=sas_format(condinfo_b.SEX, SEXF), INJURY=sas_format(condinfo_b.INJURY, YESNO)), 140, 190,
   COLS)
