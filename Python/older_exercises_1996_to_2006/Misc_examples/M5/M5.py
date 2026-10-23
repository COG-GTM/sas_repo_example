"""Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M5/M5.sas

/

PROGRAM:       C://MEPS/PROG/M5.SAS

DESCRIPTION:   THIS EXAMPLE SHOWS THE DIFFERENCE BETWEEN TWO USES OF
               THE TERM "PRIORITY CONDITION" IN MEPS.

               THE MEPS SUPPLEMENTAL PRIORITY CONDITIONS (PC) SECTION
               ASKS (IN ROUNDS 2/4), FOR EACH PERSON, ABOUT THE
               EXISTENCE OF CERTAIN PRIORITY CONDITIONS (SEE PAGES C-56 -
               C-60 OF THE HC-097 DOCUMENTATION FILE).
               INFORMATION COLLECTED IN THE PC SECTION OF THE MEPS
               IS DISTINCT FROM THAT INCLUDED IN THE CONDITION
               ROSTER.  THESE CONDITIONS ARE NOT PART OF THE CONDITION
               ROSTER UNLESS THEY ARE MENTIONED ELSEWHERE IN THE
               INTERVIEW. NOTE THAT PC QUESTIONS ARE OF THE FORM,
               "HAS PERSON *EVER* BEEN TOLD BY A DOCTOR OR OTHER
               HEALTH PROFESSIONAL THAT THEY HAVE (CONDITION)?"
               THE VARIABLES ASSOCIATED WITH THESE PC SECTION
               QUESTIONS ARE ON THE FULL-YEAR FILE (HC-097 FOR 2005).

               THE CONDITIONS FILE ALSO HAS A SET OF CONDITIONS
               FLAGGED AS PRIORITY BY THE VARIABLE PRIOLIST (SEE APPENDIX
               4 OF THE CONDITIONS FILE DOCUMENTATION).  THESE
               CONDITIONS MIGHT COME FROM A REPORT OF BEING BOTHERED BY THE
               CONDITION DURING THE YEAR, FROM AN ASSOCIATED
               EVENT OR PRESCRIPTION MEDICINE IN THE YEAR, FROM BEING
               ASSOCIATED WITH A DISABILITY DAY DURING THE YEAR, OR AS A
               FOLLOW-UP QUESTION FOR A PREVIOUSLY REPORTED CONDITION WHERE
               THE PRIORITY LIST FLAG IS SET (PRIOLIST=1).  SETTING THE
               PRIORITY LIST FLAG IS A MANUAL PROCESS AND MAY NOT HAVE BEEN
               SET FOR ALL ELIGIBLE CONDITIONS.

INPUT FILES:   (1) <MEPS_DIR>/DATA/H96.SAS7BDAT (2005 MEDICAL CONDITIONS FILE)
               (2) <MEPS_DIR>/DATA/H97.SAS7BDAT (2005 FULL-YEAR DATA FILE)


/

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

ICD9F = {"250": "DIABETES MELLITUS*", "401": "ESSENTIAL HYPERTENSION", "492": "EMPHYSEMA*", "493": "ASTHMA"}
YESNOF = {-1: "-1 INAPPLICABLE", 1: "1 YES", 0: "0  NO"}
PRIOF = {1: "1 YES", 2: "2 NO"}
CODES = {"250": "DIABETES", "401": "HYPERTENSION", "492": "EMPHYSEMA", "493": "ASTHMA"}

# 2005 conditions file (HC-096): diabetes, hypertension, emphysema, asthma -----------------
cond2005 = read_meps("h96", ["DUPERSID", "CONDIDX", "ICD9CODX", "PRIOLIST"])
cond2005 = cond2005[cond2005.ICD9CODX.isin(list(ICD9F))]
t = cond2005.assign(ICD9CODX=sas_format(cond2005.ICD9CODX, ICD9F), PRIOLIST=sas_format(cond2005.PRIOLIST, PRIOF))
print(freq_list(t, ["ICD9CODX"]).to_string(index=False), "\n")
print(freq_list(t, ["ICD9CODX", "PRIOLIST"]).to_string(index=False), "\n")

# Person-level indicators (RETAIN / FIRST. / LAST. -> groupby().max()) ---------------------
condpers = pd.DataFrame({f"C_{n}": (cond2005.ICD9CODX == c).groupby(cond2005.DUPERSID).max().astype(int)
                         for c, n in CODES.items()}).reset_index()
C = ["C_DIABETES", "C_ASTHMA", "C_HYPERTENSION", "C_EMPHYSEMA"]
for v in C:
    print(freq_list(condpers.assign(**{v: sas_format(condpers[v], YESNOF)}), [v]).to_string(index=False), "\n")

# 2005 full-year file (HC-097) priority-condition indicators (2 or < -1 -> 0) --------------
fy2005 = read_meps("h97", ["DUPERSID", "DIABDX53", "ASTHDX53", "HIBPDX53", "EMPHDX53"]).rename(columns={
    "DIABDX53": "PC_DIABETES", "ASTHDX53": "PC_ASTHMA", "HIBPDX53": "PC_HYPERTENSION", "EMPHDX53": "PC_EMPHYSEMA"})
PC = ["PC_DIABETES", "PC_ASTHMA", "PC_HYPERTENSION", "PC_EMPHYSEMA"]
for v in PC:
    fy2005[v] = fy2005[v].mask((fy2005[v] == 2) | (fy2005[v] < -1), 0)
    print(freq_list(fy2005.assign(**{v: sas_format(fy2005[v], YESNOF)}), [v]).to_string(index=False), "\n")

# MERGE CONDPERS (IN=A) FY2005 (IN=B) BY DUPERSID; missing indicators -> 0; IF B -------
fycond = condpers.merge(fy2005, on="DUPERSID", how="outer", indicator=True)
m = fycond._merge.value_counts()
print(f"BOTH={m['both']} JUSTA={m['left_only']} JUSTB={m['right_only']}\n")
fycond[C] = fycond[C].fillna(0)
fycond = fycond[fycond._merge != "left_only"]
for a, b in zip(C, PC):
    t = fycond.assign(**{a: sas_format(fycond[a], YESNOF), b: sas_format(fycond[b], YESNOF)})
    print(freq_list(t, [a, b]).to_string(index=False), "\n")
