# --------------------------------------------------------------------------
# PROGRAM:       C:\MEPS\PROG\M5.SAS
#
# DESCRIPTION:   THIS EXAMPLE SHOWS THE DIFFERENCE BETWEEN TWO USES OF
#                THE TERM "PRIORITY CONDITION" IN MEPS.
#                THE MEPS SUPPLEMENTAL PRIORITY CONDITIONS (PC) SECTION
#                ASKS (IN ROUNDS 2/4), FOR EACH PERSON, ABOUT THE
#                EXISTENCE OF CERTAIN PRIORITY CONDITIONS (SEE PAGES C-56 -
#                C-60 OF THE HC-097 DOCUMENTATION FILE).
#                INFORMATION COLLECTED IN THE PC SECTION OF THE MEPS
#                IS DISTINCT FROM THAT INCLUDED IN THE CONDITION
#                ROSTER.  THESE CONDITIONS ARE NOT PART OF THE CONDITION
#                ROSTER UNLESS THEY ARE MENTIONED ELSEWHERE IN THE
#                INTERVIEW. NOTE THAT PC QUESTIONS ARE OF THE FORM,
#                "HAS PERSON *EVER* BEEN TOLD BY A DOCTOR OR OTHER
#                HEALTH PROFESSIONAL THAT THEY HAVE (CONDITION)?"
#                THE VARIABLES ASSOCIATED WITH THESE PC SECTION
#                QUESTIONS ARE ON THE FULL-YEAR FILE (HC-097 FOR 2005).
#                THE CONDITIONS FILE ALSO HAS A SET OF CONDITIONS
#                FLAGGED AS PRIORITY BY THE VARIABLE PRIOLIST (SEE APPENDIX
#                4 OF THE CONDITIONS FILE DOCUMENTATION).  THESE
#                CONDITIONS MIGHT COME FROM A REPORT OF BEING BOTHERED BY THE
#                CONDITION DURING THE YEAR, FROM AN ASSOCIATED
#                EVENT OR PRESCRIPTION MEDICINE IN THE YEAR, FROM BEING
#                ASSOCIATED WITH A DISABILITY DAY DURING THE YEAR, OR AS A
#                FOLLOW-UP QUESTION FOR A PREVIOUSLY REPORTED CONDITION WHERE
#                THE PRIORITY LIST FLAG IS SET (PRIOLIST=1).  SETTING THE
#                PRIORITY LIST FLAG IS A MANUAL PROCESS AND MAY NOT HAVE BEEN
#                SET FOR ALL ELIGIBLE CONDITIONS.
#
# INPUT FILES:   (1) C:\MEPS\DATA\H96.SAS7BDAT (2005 MEDICAL CONDITIONS FILE)
#                (2) C:\MEPS\DATA\H97.SAS7BDAT (2005 FULL-YEAR DATA FILE)
#
# Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M5/M5.sas
# --------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, crosstab_list, load_meps  # noqa: E402

pd.set_option("display.width", 200)

ICD9F = {"250": "DIABETES MELLITUS*", "401": "ESSENTIAL HYPERTENSION", "492": "EMPHYSEMA*", "493": "ASTHMA"}
YESNOF = {-1: "-1 INAPPLICABLE", 1: "1 YES", 0: "0  NO"}
PRIOF = {1: "1 YES", 2: "2 NO"}
CONDS = {"250": "DIABETES", "401": "HYPERTENSION", "492": "EMPHYSEMA", "493": "ASTHMA"}


def freq(df, cols, fmts):
    t = crosstab_list(df.assign(**{c: apply_format(df[c], fmts[c], missing=".") for c in cols if c in fmts}), cols)
    t["PERCENT"] = (100 * t["COUNT"] / t["COUNT"].sum()).round(2)
    print(t.to_string(index=False), "\n")


# 2005 conditions file: diabetes, essential hypertension, emphysema, asthma
cond = load_meps("h96", columns=["DUPERSID", "CONDIDX", "ICD9CODX", "PRIOLIST"])
cond = cond[cond["ICD9CODX"].isin(list(ICD9F))].copy()
freq(cond, ["ICD9CODX"], {"ICD9CODX": ICD9F})
freq(cond, ["ICD9CODX", "PRIOLIST"], {"ICD9CODX": ICD9F, "PRIOLIST": PRIOF})

# Person-level indicators for each type of priority condition (condition roster)
condpers = (pd.crosstab(cond["DUPERSID"], cond["ICD9CODX"]).reindex(columns=list(CONDS), fill_value=0) > 0).astype(int)
condpers.columns = [f"C_{CONDS[c]}" for c in condpers.columns]
condpers = condpers.reset_index()
for c in ["C_DIABETES", "C_ASTHMA", "C_HYPERTENSION", "C_EMPHYSEMA"]:
    freq(condpers, [c], {c: YESNOF})

# Full-year file PC section indicators: 2 (no) or < -1 (DK/refused/NA) -> 0
ren = {"DIABDX53": "PC_DIABETES", "ASTHDX53": "PC_ASTHMA", "HIBPDX53": "PC_HYPERTENSION", "EMPHDX53": "PC_EMPHYSEMA"}
fy = load_meps("h97", columns=["DUPERSID", *ren, "VARPSU", "VARSTR", "PERWT05F"]).rename(columns=ren)
for c in ren.values():
    fy[c] = np.where((fy[c] == 2) | (fy[c] < -1), 0, fy[c])
    freq(fy, [c], {c: YESNOF})

# Merge conditions and full-year files to compare priority condition information
m = condpers.merge(fy, on="DUPERSID", how="outer", indicator=True)
k = m["_merge"].value_counts()
print(f"BOTH={k.get('both', 0)} JUSTA={k.get('left_only', 0)} JUSTB={k.get('right_only', 0)}\n")
fycond = m[m["_merge"] != "left_only"].copy()
ccols = ["C_DIABETES", "C_HYPERTENSION", "C_EMPHYSEMA", "C_ASTHMA"]
fycond[ccols] = fycond[ccols].fillna(0)
for c in ["DIABETES", "ASTHMA", "HYPERTENSION", "EMPHYSEMA"]:
    freq(fycond, [f"C_{c}", f"PC_{c}"], {f"C_{c}": YESNOF, f"PC_{c}": YESNOF})
