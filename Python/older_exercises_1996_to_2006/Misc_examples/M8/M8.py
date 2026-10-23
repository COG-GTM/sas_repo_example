"""Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M8/M8.sas

/

PROGRAM:     <MEPS_DIR>/PROG/M8.SAS

DESCRIPTION: THIS EXAMPLE SHOWS HOW TO COMPUTE PRESCRIBED
                 MEDICINE EXPENDITURES ASSOCIATED WITH CANCER
                 CONDITIONS.

                 IN ADDITION TO THE MEDICAL CONDITIONS AND FULL-
                 YEAR FILES, THE PRESCRIBED MEDICINES EVENT FILE AND
                 THE CLNK FILE ARE USED.
                 THE PMED EXPENDITURES ARE SUMMED TO THE EVENT
                 LEVEL.
                 MODIFIED SOURCE OF PAYMENT (SOP) CATEGORIES
                 ARE CREATED AS COLUMN CATEGORIES.
                 ROW CATEGORIES ARE AGE AND RACE/ETHNICITY (FROM
                 THE FULL-YEAR FILE).
                 USE PROC TABULATE TO COMPUTE AND DISPLAY OUTPUT.

INPUT FILES: (1) <MEPS_DIR>/DATA/H97.SAS7BDAT (2005 FULL-YEAR DATA FILE)
                 (2) <MEPS_DIR>/DATA/H96.SAS7BDAT (2005 MEDICAL CONDITIONS FILE)
                 (3) <MEPS_DIR>/DATA/H94I1.SAS7BDAT (2005 CLNK FILE)
                           THIS FILE LINKS CONDITIONS TO EVENTS
                 (4) <MEPS_DIR>/DATA/H94A.SAS7BDAT (2005 PRESCRIBED MED. FILE)

/

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

AGECAT = {1: " < 18", 2: "18-64", 3: "65+"}
RACETHNB = {1: "HISPANIC", 2: "BLACK", 3: "ASIAN", 4: "OTHER"}
IDS = ["32663024", "33634027"]
EXP = ["TOTRX05", "OOP", "PRIVATE", "MEDICARE", "MEDICAID", "OTHER"]

# 2005 conditions (HC-096) with clinical classification codes 011-045 (cancers) -----------
cond05 = read_meps("h96", ["DUPERSID", "CCCODEX", "CONDIDX"])
cond05 = cond05.loc[cond05.CCCODEX.between("011", "045"), ["DUPERSID", "CONDIDX"]]

# 2005 prescribed medicine acquisitions (HC-094A) --------------------------------------------
pmedacq = read_meps("h94a", ["DUPERSID", "RXRECIDX", "LINKIDX", "RXSF05X", "RXMD05X", "RXPV05X", "RXMR05X",
                             "RXXP05X", "RXNDC"]).sort_values(["LINKIDX", "RXRECIDX"])
print(pmedacq.loc[pmedacq.DUPERSID.isin(IDS), ["DUPERSID", "LINKIDX", "RXRECIDX", "RXXP05X", "RXSF05X", "RXPV05X",
                                               "RXMD05X", "RXMR05X"]]
      .rename(columns={"RXSF05X": "OOP", "RXMD05X": "MEDICAID", "RXMR05X": "MEDICARE", "RXPV05X": "PRIVATE"})
      .to_string(index=False), "\n")

# Sum acquisitions to the prescribed medicine event (FIRST./LAST. LINKIDX accumulators) ---
g = pmedacq.groupby("LINKIDX", sort=True)
pmedevnt = g.agg(DUPERSID=("DUPERSID", "last"), RXNDC=("RXNDC", "last"), OOP=("RXSF05X", "sum"),
                 MEDICARE=("RXMR05X", "sum"), MEDICAID=("RXMD05X", "sum"), PRIVATE=("RXPV05X", "sum"),
                 TOTRX05=("RXXP05X", "sum")).rename_axis("EVNTIDX").reset_index()
oth = pmedevnt.TOTRX05 - (pmedevnt.MEDICARE + pmedevnt.MEDICAID + pmedevnt.PRIVATE + pmedevnt.OOP)
pmedevnt["OTHER"] = np.sign(oth) * np.floor(oth.abs() + 0.5)  # SAS ROUND (half away from zero)
print(pmedevnt.loc[pmedevnt.DUPERSID.isin(IDS), ["DUPERSID", "EVNTIDX", "RXNDC"] + EXP].to_string(index=False), "\n")

# 2005 condition-event link file: HC-094I1 has been re-released as HC-094IF1 --------------
try:
    clnk = read_meps("h94i1", ["CONDIDX", "EVNTIDX"], download=False)
except FileNotFoundError:
    clnk = read_meps("h94if1", ["CONDIDX", "EVNTIDX"])
condclnk = cond05.merge(clnk, on="CONDIDX", how="left")                    # IF A
condpmed = condclnk.merge(pmedevnt.drop(columns="DUPERSID"), on="EVNTIDX", how="inner")  # IF A AND B
condpmed = condpmed.sort_values(["EVNTIDX", "CONDIDX"])
COLS = ["DUPERSID", "CONDIDX", "EVNTIDX"] + EXP
print(condpmed.loc[condpmed.DUPERSID.isin(IDS), COLS].to_string(index=False), "\n")
# PROC SORT NODUPKEY BY EVNTIDX: count each PMED event once even if linked to 2+ cancers
condpmed = condpmed.drop_duplicates("EVNTIDX")
print(condpmed.loc[condpmed.DUPERSID.isin(IDS), COLS].to_string(index=False), "\n")

# 2005 full-year file (HC-097): age category and race/ethnicity -----------------------------
puf97 = read_meps("h97", ["DUPERSID", "AGE05X", "AGE53X", "AGE42X", "AGE31X", "RACETHNX", "PERWT05F"])
p = puf97
age = np.select([p.AGE05X >= 0, p.AGE53X >= 0, p.AGE42X >= 0, p.AGE31X >= 0], [p.AGE05X, p.AGE53X, p.AGE42X,
                                                                                p.AGE31X], p.AGE05X)
p["AGECAT"] = np.select([(age >= 0) & (age < 18), (age >= 18) & (age < 65), age >= 65], [1, 2, 3], np.nan)
temp = condpmed.merge(p[["DUPERSID", "AGECAT", "RACETHNX", "PERWT05F"]], on="DUPERSID", how="left")
temp[EXP] = temp[EXP] / 1000

# PROC TABULATE MISSING ... WEIGHT PERWT05F: weighted SUMs ($ thousands) ------------------
temp["AGECAT"] = sas_format(temp.AGECAT, AGECAT)
temp["RACETHNX"] = sas_format(temp.RACETHNX, RACETHNB)
w = temp[EXP].mul(temp.PERWT05F, axis=0)
rows = [pd.DataFrame({"CLASS": ["TOTAL"], "LEVEL": [""], **{c: [w[c].sum()] for c in EXP}})]
for c in ["AGECAT", "RACETHNX"]:
    rows.append(w.groupby(temp[c], dropna=False).sum().rename_axis("LEVEL").reset_index().assign(CLASS=c))
show("2005 PMED expenditures for cancer, $ thousands", pd.concat(rows)[["CLASS", "LEVEL"] + EXP], "{:,.0f}")
