# ----------------------------------------------------------------------
# PROGRAM:      C:\MEPS\PROG\M3.SAS
#
# DESCRIPTION:  THIS EXAMPLE SHOWS THE USE OF ID VARIABLES ON DIFFERENT
#               MEPS PUBLIC-USE FILES (PUFs) AND HOW TO USE THESE ID
#               VARIABLES TO MERGE MEPS FILES.
#               THIS EXAMPLE SHOWS PRINTS OF PERSON-LEVEL AND CONDITION-
#               LEVEL FILES TO SHOW THE USE OF THE ID VARIABLES.
#               DUPERSID (HC-097) IS A PERSON-LEVEL IDENTIFIER.
#               CONDIDX (HC-096) IS A PERSON-CONDITION-LEVEL IDENTIFIER.
#               NOTE THAT THE MERGES USING DUPERSID ALSO USE PANEL, THAT IS,
#               MERGE BY DUPERSID PANEL.  WHILE NOT NECESSARY WHEN PROCESSING
#               ONLY ONE YEAR OF MEPS DATA, AS IN THIS EXAMPLE, IT IS GOOD
#               PROGRAMMING PRACTICE TO ADD PANEL TO MERGES BECAUSE
#               WHEN PROCESSING DATA ACROSS SEVERAL MEPS YEARS, DUPERSID (AND
#               OTHER IDS) MAY NOT BE UNIQUE. SPECIFICALLY, STARTING WITH MEPS 2004,
#               PANEL 9, IDS DUPLICATE EARLIER YEARS.
#
# INPUT FILE:   (1) C:\MEPS\DATA\H97.SAS7BDAT (2005 FULL-YEAR DATA FILE)
#               (2) C:\MEPS\DATA\H96.SAS7BDAT (2005 CONDITIONS FILE)
#
# Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M3/M3.sas
# ----------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, load_meps  # noqa: E402

pd.set_option("display.width", 200)

YESNO = {-9: "-9 NOT ASCERTAINED", -1: "-1 INAPPLICABLE", 1: "1 YES", 2: "2 NO"}
SEXF = {1: "1 MALE", 2: "2 FEMALE"}


def show(df, cols, first, last):
    """PROC PRINT (FIRSTOBS=first OBS=last) with SEX / INJURY formats."""
    out = df.iloc[first - 1 : last][cols].copy()
    if "SEX" in out:
        out["SEX"] = apply_format(out["SEX"], SEXF, missing=".")
    if "INJURY" in out:
        out["INJURY"] = apply_format(out["INJURY"], YESNO, missing=".")
    print(out.to_string(), "\n")


# DUPERSID and other variables from HC-097
FY2005 = load_meps("h97", columns=["DUID", "PID", "DUPERSID", "PANEL", "AGE05X", "SEX", "PERWT05F", "VARSTR", "VARPSU"])
FY2005 = FY2005.sort_values(["DUPERSID", "PANEL"], kind="stable").reset_index(drop=True)
show(FY2005, ["DUID", "PID", "DUPERSID", "PANEL", "AGE05X", "SEX"], 40, 90)

# DUPERSID, CONDIDX and other variables from HC-096
COND2005 = load_meps("h96", columns=["DUPERSID", "PANEL", "CONDN", "CONDIDX", "INJURY", "ICD9CODX"])
COND2005 = COND2005.sort_values(["DUPERSID", "PANEL", "CONDIDX"], kind="stable").reset_index(drop=True)
show(COND2005, ["DUPERSID", "PANEL", "CONDN", "CONDIDX", "INJURY", "ICD9CODX"], 140, 190)

# Merge files to connect person info with condition info (match on DUPERSID PANEL)
cols = ["DUPERSID", "PANEL", "CONDIDX", "AGE05X", "SEX", "INJURY", "ICD9CODX"]
key = ["DUPERSID", "PANEL"]
CONDINFO = FY2005.merge(COND2005, on=key, how="outer").sort_values(key, kind="stable").reset_index(drop=True)
show(CONDINFO, cols, 140, 190)

# Merge files but now only keep matching records (IF A AND B)
CONDINFO_B = FY2005.merge(COND2005, on=key, how="inner").sort_values(key, kind="stable").reset_index(drop=True)
show(CONDINFO_B, cols, 140, 190)
