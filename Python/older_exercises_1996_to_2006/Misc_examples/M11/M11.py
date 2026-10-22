# --------------------------------------------------------------------
# PROGRAM:      C:\MEPS\PROG\EXAMPLE11.SAS
#
# DESCRIPTION:  THIS EXAMPLE SHOWS THE PROCESS FOR MERGING PARENT'S
#               INFORMATION TO CHILDREN'S RECORDS. IN THIS CASE, THE MOTHER'S
#               AND FATHER'S EMPLOYMENT STATUS (EMPST31) IS MERGED TO
#               CHILDREN, AGES 0-17. A NEW VARIABLE PAR_WORK IS CONSTURCTED TO
#               SUMMARIZE IF THE CHILD HAS TWO WORKING PARENTS, ONE
#               WORKING PARENT, OR NO WORKING PARENTS.
#               RUNS SURVEYFREQ THE NEW VARIABLE PAR_WORK.
#
# INPUT FILE:   C:\MEPS\DATA\H105.SAS7BDAT (2006 FULL-YEAR DATA FILE)
#
# Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M11/M11.sas
# --------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, crosstab_list, load_meps, svy_freq  # noqa: E402

pd.set_option("display.width", 200)

KIDFMT = {1: "CHILD, AGE 0-17", 0: "NOT A CHILD"}
PAR_WORK = {1: "1 BOTH PARENTS WORK", 2: "2 ONE PARENT WORK", 3: "3 NO WORKING PARENT", 0: "0 NOT A CHILD"}
NR = {k: "NOT REPORTED" for k in (-9, -8, -7, -1)}
MOMEMP = NR | {0: "NO MOM IN MEPS", 1: "EMPLOYED", 2: "NOT EMPLOYED", 3: "NOT EMPLOYED", 4: "NOT EMPLOYED"}
DADEMP = NR | {0: "NO DAD IN MEPS", 1: "1 EMPLOYED", 2: "NOT EMPLOYED", 3: "NOT EMPLOYED", 4: "NOT EMPLOYED"}

ALL = load_meps("h105", columns=["DUPERSID", "DUID", "PID", "AGE31X", "MOPID31X", "DAPID31X", "EMPST31", "PERWT06F",
                                 "VARSTR", "VARPSU"])  # fmt: skip
ALL = ALL[ALL["PERWT06F"] > 0].copy()
print(ALL[["DUID", "DUPERSID", "AGE31X", "MOPID31X", "DAPID31X", "EMPST31"]].head(50).to_string(index=False), "\n")

# Kids dataset with linking IDs (DUID || PID) to both mom and dad
kids = ALL[ALL["AGE31X"].between(0, 17)][["DUPERSID", "DUID", "MOPID31X", "DAPID31X", "AGE31X"]].copy()
duid = kids["DUID"].astype(int).astype(str).str.zfill(5)
kids["MOMLINK"] = np.where(kids["MOPID31X"] != -1, duid + kids["MOPID31X"].astype(int).astype(str).str.zfill(3), "")
kids["DADLINK"] = np.where(kids["DAPID31X"] != -1, duid + kids["DAPID31X"].astype(int).astype(str).str.zfill(3), "")
kids = kids.drop(columns="DUID")
print(kids.head(50).to_string(index=False), "\n")

# "Dads" and "moms": DUPERSID renamed to the link IDs, keeping EMPST31
dads = ALL[["DUPERSID", "EMPST31"]].rename(columns={"DUPERSID": "DADLINK", "EMPST31": "DAD_EMPST31"})
moms = ALL[["DUPERSID", "EMPST31"]].rename(columns={"DUPERSID": "MOMLINK", "EMPST31": "MOM_EMPST31"})

kids2 = kids.merge(dads, on="DADLINK", how="left")
kids2["DAD_EMPST31"] = kids2["DAD_EMPST31"].fillna(0)
kw = kids2.merge(moms, on="MOMLINK", how="left")
kw["MOM_EMPST31"] = kw["MOM_EMPST31"].fillna(0)
print(kw.sort_values("DUPERSID").head(50).to_string(index=False), "\n")

# Merge back to ALL to pick up the entire sample (keeps the full design)
all2 = ALL.merge(kw.drop(columns=["MOPID31X", "DAPID31X", "AGE31X"]), on="DUPERSID", how="left", indicator=True)
kid = all2.pop("_merge") == "both"
all2["POP_KID"] = kid.astype(int)
mom1, dad1 = all2["MOM_EMPST31"] == 1, all2["DAD_EMPST31"] == 1
all2["PAR_WORK"] = np.select([~kid, mom1 & dad1, mom1 | dad1], [0, 1, 2], default=3)  # NUMBER OF WORKING PARENTS
all2.loc[~kid, ["DAD_EMPST31", "MOM_EMPST31"]] = -1
print(all2[["DUPERSID", "AGE31X", "MOMLINK", "DADLINK", "PAR_WORK", "MOM_EMPST31", "DAD_EMPST31"]].head(50).to_string(index=False))

fmt = all2.assign(
    POP_KID=apply_format(all2["POP_KID"], KIDFMT), PAR_WORK=apply_format(all2["PAR_WORK"], PAR_WORK),
    MOM_EMPST31=apply_format(all2["MOM_EMPST31"], MOMEMP), DAD_EMPST31=apply_format(all2["DAD_EMPST31"], DADEMP),
)  # fmt: skip
print(crosstab_list(fmt, ["POP_KID", "PAR_WORK", "MOM_EMPST31", "DAD_EMPST31"]).to_string(index=False), "\n")

# PROC SURVEYFREQ; TABLES POP_KID*PAR_WORK / ROW
print("TABLE POP_KID*PAR_WORK / ROW")
print(svy_freq(fmt, "PAR_WORK", domain="POP_KID", weight="PERWT06F", strata="VARSTR", psu="VARPSU").to_string(index=False))
