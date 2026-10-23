"""Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M11/M11.sas

/

PROGRAM:        <MEPS_DIR>/PROG/EXAMPLE11.SAS

DESCRIPTION:    THIS EXAMPLE SHOWS THE PROCESS FOR MERGING PARENT'S
        INFORMATION TO CHILDREN'S RECORDS. IN THIS CASE, THE MOTHER'S
        AND FATHER'S EMPLOYMENT STATUS (EMPST31) IS MERGED TO
        CHILDREN, AGES 0-17. A NEW VARIABLE PAR_WORK IS CONSTURCTED TO
        SUMMARIZE IF THE CHILD HAS TWO WORKING PARENTS, ONE
        WORKING PARENT, OR NO WORKING PARENTS.

                                RUNS SURVEYFREQ THE NEW VARIABLE PAR_WORK.


INPUT FILE:     <MEPS_DIR>/DATA/H105.SAS7BDAT (2006 FULL-YEAR DATA FILE)

/

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

KIDFMT = {1: "CHILD, AGE 0-17", 0: "NOT A CHILD"}
PAR_WORK = {1: "1 BOTH PARENTS WORK", 2: "2 ONE PARENT WORK", 3: "3 NO WORKING PARENT", 0: "0 NOT A CHILD"}
MOMEMP = {(-9, -8, -7, -1): "NOT REPORTED", 0: "NO MOM IN MEPS", 1: "EMPLOYED", (2, 3, 4): "NOT EMPLOYED"}
DADEMP = {(-9, -8, -7, -1): "NOT REPORTED", 0: "NO DAD IN MEPS", 1: "1 EMPLOYED", (2, 3, 4): "NOT EMPLOYED"}

# 2006 consolidated full-year file (HC-105), positive-weight persons -----------------------
all_fy06 = read_meps("h105", ["DUPERSID", "DUID", "PID", "AGE31X", "MOPID31X", "DAPID31X", "EMPST31", "PERWT06F",
                              "VARSTR", "VARPSU"])
all_fy06 = all_fy06[all_fy06.PERWT06F > 0].reset_index(drop=True)
print(all_fy06[["DUID", "DUPERSID", "AGE31X", "MOPID31X", "DAPID31X", "EMPST31"]].head(50).to_string(), "\n")

# Kids with links to mom and dad: PUT(DUID,Z5.)||PUT(PID,Z3.) -----------------------------
kids = all_fy06[all_fy06.AGE31X.between(0, 17)].copy()
duid = kids.DUID.astype(int).astype(str).str.zfill(5)
kids["MOMLINK"] = np.where(kids.MOPID31X != -1, duid + kids.MOPID31X.astype(int).astype(str).str.zfill(3), "")
kids["DADLINK"] = np.where(kids.DAPID31X != -1, duid + kids.DAPID31X.astype(int).astype(str).str.zfill(3), "")
kids = kids[["DUPERSID", "MOMLINK", "DADLINK", "MOPID31X", "DAPID31X", "AGE31X"]]
print(kids.head(50).to_string(), "\n")

dads = all_fy06[["DUPERSID", "EMPST31"]].rename(columns={"DUPERSID": "DADLINK", "EMPST31": "DAD_EMPST31"})
moms = all_fy06[["DUPERSID", "EMPST31"]].rename(columns={"DUPERSID": "MOMLINK", "EMPST31": "MOM_EMPST31"})
print(dads.sort_values("DADLINK").head(50).to_string(), "\n")
print(moms.sort_values("MOMLINK").head(50).to_string(), "\n")

# MERGE KIDS (IN=A) DADS (IN=B) BY DADLINK; IF A; unmatched -> 0. Then the same for moms.
kids2 = kids.merge(dads, on="DADLINK", how="left")
kids2["DAD_EMPST31"] = kids2.DAD_EMPST31.fillna(0)
kids_wp = kids2.merge(moms, on="MOMLINK", how="left")
kids_wp["MOM_EMPST31"] = kids_wp.MOM_EMPST31.fillna(0)
kids_wp = kids_wp.sort_values("DUPERSID")
print(kids_wp.head(50).to_string(index=False), "\n")

# Merge back to the full sample; parents' work status for kids --------------------------
all2 = all_fy06.drop(columns=["AGE31X", "MOPID31X", "DAPID31X"]).merge(kids_wp, on="DUPERSID", how="left",
                                                                       indicator=True).sort_values("DUPERSID")
kid = all2._merge == "both"
all2["POP_KID"] = kid.astype(int)
both_work = (all2.MOM_EMPST31 == 1) & (all2.DAD_EMPST31 == 1)
one_work = (all2.MOM_EMPST31 == 1) | (all2.DAD_EMPST31 == 1)
all2["PAR_WORK"] = np.select([~kid, both_work, one_work], [0, 1, 2], 3)
all2.loc[~kid, ["DAD_EMPST31", "MOM_EMPST31"]] = -1
print(all2[["DUPERSID", "AGE31X", "MOMLINK", "DADLINK", "PAR_WORK", "MOM_EMPST31", "DAD_EMPST31"]].head(50)
      .to_string(index=False), "\n")

t = all2.assign(POP_KID=sas_format(all2.POP_KID, KIDFMT), PAR_WORK=sas_format(all2.PAR_WORK, PAR_WORK),
                MOM_EMPST31=sas_format(all2.MOM_EMPST31, MOMEMP), DAD_EMPST31=sas_format(all2.DAD_EMPST31, DADEMP))
print(freq_list(t, ["POP_KID", "PAR_WORK", "MOM_EMPST31", "DAD_EMPST31"]).to_string(index=False), "\n")

# PROC SURVEYFREQ ... TABLES POP_KID*PAR_WORK / ROW
show("POP_KID * PAR_WORK (row percents)", surveyfreq(t, ["POP_KID", "PAR_WORK"], "PERWT06F"), "{:,.4f}")
