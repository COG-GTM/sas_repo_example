"""Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M7/M7.sas

/

PROGRAM:       <MEPS_DIR>/PROG/EXAMPLE07.SAS

DESCRIPTION:      THIS EXAMPLE SHOWS HOW TO COMPUTE PERSON-LEVEL
               PRESCRIBED MEDICINE EXPENDITURES FOR PERSONS
               WITH AT LEAST ONE PMED EVENT.

INPUT FILES:      <MEPS_DIR>/DATA/H77A.SAS7BDAT (2003 PMED EVENTS FILE)
               <MEPS_DIR>/DATA/H79.SAS7BDAT (2003 FULL-YEAR DATA FILE)

/

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

AGECAT = {1: "UNDER 18", 2: "18 - 64", 3: "65 AND OLDER"}
RACETHN = {1: "HISPANIC", 2: "BLACK", 3: "ASIAN", 4: "OTHER"}
INSCOV = {1: "ANY PRIVATE", 2: "PUBLIC ONLY", 3: "UNINSURED"}
RX = ["RXEXP03", "RXSLF03", "RXPRV03", "RXMCR03", "RXMCD03"]

# Persons with a 2003 prescribed medicine purchase (HC-077A, NODUPKEY BY DUPERSID) -----
puf77a = read_meps("h77a", ["DUPERSID"]).drop_duplicates()

# 2003 full-year file (HC-079) -------------------------------------------------------------
puf79 = read_meps("h79", ["DUPERSID", "AGE03X", "AGE53X", "AGE42X", "AGE31X", "RACETHNX", "PERWT03F", "VARSTR",
                          "VARPSU", "INSCOV03", "RXMCD03", "RXMCR03", "RXPRV03", "RXSLF03", "RXEXP03"])
p = puf79
p["AGE"] = np.select([p.AGE03X >= 0, p.AGE53X >= 0, p.AGE42X >= 0, p.AGE31X >= 0],
                     [p.AGE03X, p.AGE53X, p.AGE42X, p.AGE31X], p.AGE03X)
p["AGECAT"] = np.select([(p.AGE >= 0) & (p.AGE < 18), (p.AGE >= 18) & (p.AGE < 65), p.AGE >= 65], [1, 2, 3], np.nan)
p["OTHER"] = p.RXEXP03 - (p.RXMCD03 + p.RXMCR03 + p.RXPRV03 + p.RXSLF03)
for v in RX + ["OTHER"]:
    p[v + "X"] = p[v] / 1000

# MERGE PUF77A (IN=A) PUF79 BY DUPERSID; IF A -> persons with a PMED purchase
puf79 = puf77a.merge(p, on="DUPERSID", how="left")
show("RXEXP03 (persons with a PMED purchase)", surveymeans(puf79, "RXEXP03", "PERWT03F"))

# PROC TABULATE MISSING ... WEIGHT PERWT03F: weighted SUMs (in $000s) and MEANs by group
puf79["AGECAT"] = sas_format(puf79.AGECAT, AGECAT)
puf79["RACETHNX"] = sas_format(puf79.RACETHNX, RACETHN)
puf79["INSCOV03"] = sas_format(puf79.INSCOV03, INSCOV)


def tabulate(vars_, stat, title):
    w = puf79.PERWT03F
    rows = [("TOTAL", "", puf79.index)] + [(c, lvl, idx) for c in ["AGECAT", "RACETHNX", "INSCOV03"]
                                            for lvl, idx in puf79.groupby(c, dropna=False).groups.items()]
    out = []
    for c, lvl, idx in rows:
        s = puf79.loc[idx]
        tot = s[vars_].mul(w[idx], axis=0).sum()
        out.append({"CLASS": c, "LEVEL": lvl, **(tot if stat == "SUM" else tot / w[idx].sum()).to_dict()})
    show(title, pd.DataFrame(out), "${:,.0f}")


tabulate([v + "X" for v in RX + ["OTHER"]], "SUM", "2003 TOTAL PMED EXPENDITURES ($ thousands)")
tabulate(RX + ["OTHER"], "MEAN", "2003 MEAN PMED EXPENDITURES")
