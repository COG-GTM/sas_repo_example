"""Python port of SAS/older_exercises_1996_to_2006/Employment_examples/EM2/EM2.sas

/

PROGRAM:       <MEPS_DIR>/PROG/EXAMPLE_EM2.SAS

DESCRIPTION:      THIS EXAMPLE SHOWS HOW TO USE THE 2002 MEPS JOBS
               FILE (HC-063) TO DETERMINE HOW MANY PERSONS
               WORKING AT THE START OF THE YEAR CHANGED JOBS.

INPUT FILES:      (1) <MEPS_DIR>/DATA/H63.SAS7BDAT (2002 MEPS JOBS FILE)
               (2) <MEPS_DIR>/DATA/H62.SAS7BDAT (2002 MEPS FULL-YEAR FILE)

/

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403


def read_h62(columns):
    """2002 full-year file. HC-062 (weight PERWT02P) has been replaced on the MEPS site by
    HC-070 (weight PERWT02F); use HC-070 when HC-062 is not available locally."""
    try:
        return read_meps("h62", columns, download=False)
    except FileNotFoundError:
        cols = [c.replace("PERWT02P", "PERWT02F") for c in columns]
        print("NOTE: HC-062 not found; using its replacement HC-070 (PERWT02F as PERWT02P).\n")
        return read_meps("h70", cols).rename(columns={"PERWT02F": "PERWT02P"})


AGEF = {slice(18, 44): "18-44", slice(45, 64): "45-64", slice(65, 85): "65-85"}

h63 = read_meps("h63", ["DUPERSID", "PANEL", "RN", "SUBTYPE", "JSTRTM", "JSTRTD", "JSTRTY", "JOBSIDX"])
rnd = ((h63.PANEL == 6) & (h63.RN == 3)) | ((h63.PANEL == 7) & (h63.RN == 1))
jan1 = (h63.JSTRTM == 1) & (h63.JSTRTD == 1) & (h63.JSTRTY == 2002)
# Persons employed at the start of 2002 (PROC SORT NODUPKEY OUT=...(KEEP=DUPERSID))
empstart = h63.loc[rnd & h63.SUBTYPE.isin([1, 2, 3, 4]) & ((h63.JSTRTY < 2002) | jan1), "DUPERSID"].drop_duplicates()
# ... who changed jobs during 2002 (MERGE ... IF A AND B)
chg = h63[h63.DUPERSID.isin(empstart)]
chngjob = chg.loc[chg.SUBTYPE.isin([3, 4]) | (chg.SUBTYPE.isin([1, 2]) & (chg.JSTRTY == 2002) & ~jan1[chg.index]),
                  "DUPERSID"].drop_duplicates()

hc62 = read_h62(["DUPERSID", "AGE02X", "AGE42X", "AGE31X", "PERWT02P"])
hc62["AGE"] = np.select([hc62.AGE02X >= 0, hc62.AGE42X >= 0, hc62.AGE31X >= 0], [hc62.AGE02X, hc62.AGE42X,
                                                                                 hc62.AGE31X], np.nan)
hc62 = hc62.loc[hc62.AGE >= 18, ["AGE", "DUPERSID", "PERWT02P"]]
f = sas_format(hc62.AGE, AGEF).value_counts().sort_index().rename("Frequency").to_frame()
f["Percent"] = 100 * f.Frequency / f.Frequency.sum()
show("AGE", f.reset_index(), "{:,.2f}")

# MERGE HC62 (IN=A) EMPSTART_POP (IN=B) CHNGJOB_POP (IN=C); IF A -------------------------
chnginfo = hc62.assign(EMPSTART=np.where(hc62.DUPERSID.isin(empstart), "YES", "NO"),
                       CHNGJOB=np.where(hc62.DUPERSID.isin(chngjob), "YES", "NO"))
for cols in (["EMPSTART"], ["CHNGJOB"], ["EMPSTART", "CHNGJOB"], ["CHNGJOB"]):
    print(freq_list(chnginfo, cols).to_string(index=False), "\n")
s = chnginfo[chnginfo.EMPSTART == "YES"]
f = s.groupby("CHNGJOB").PERWT02P.sum().rename("Frequency").to_frame()
f["Percent"] = 100 * f.Frequency / f.Frequency.sum()
show("CHNGJOB, persons employed at the start of 2002 (WEIGHT PERWT02P)", f.reset_index(), "{:,.2f}")
