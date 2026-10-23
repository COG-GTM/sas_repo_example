"""Python port of SAS/older_exercises_1996_to_2006/Employment_examples/EM1/EM1.sas

/

PROGRAM:       <MEPS_DIR>/PROG/EXAMPLE_EM1.SAS

DESCRIPTION:      THIS EXAMPLE SHOWS HOW TO BUILD AN ANALYTIC FILE
               AND CREATE NEW VARIABLES TO EXAMINE THE
               RELATIONSHIP BETWEEN PERCEIVED HEALTH STATUS
               AND A PERSON'S WEEKLY EARNINGS, WHERE WEEKLY
               EARNINGS ARE THOSE OF THE CURRENT MAIN JOB.

               PERSON-LEVEL RECORDS ARE DIVIDED INTO QUARTILES
               BASED ON WEEKLY EARNINGS AND PERSON-LEVEL WEIGHTS.
               THE RESULT IS 4 EQUALLY WEIGHTED QUARTILES FOR
               WEEKLY EARNINGS.

INPUT FILE:      <MEPS_DIR>/DATA/H62.SAS7BDAT (2002 FULL-YEAR POPULATION)
                     -- PERSON-LEVEL FILE

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


HLTHF = {1: "1 EXCELLENT", 2: "2 VERY GOOD", 3: "3 GOOD", 4: "4 FAIR", 5: "5 POOR"}
QUARTF = {1: "   1 LOWEST ", 2: "   2        ", 3: "   3        ", 4: "   4 HIGHEST"}
WKLYF = {slice(0.13, 49.99): "    0.13 -  49.99", slice(50, 499.99): "   50.00 - 499.99",
         slice(500, 999.99): "  500.00 - 999.99", slice(1000, None): "1,000.00+        "}

h62 = read_h62(["DUPERSID", "PERWT02P", "VARSTR", "VARPSU", "EMPST42", "SELFCM42", "SELFCM31", "HRWG42X",
                "HRWG31X", "HOUR42", "HOUR31", "RTHLTH42", "TEMPJB42", "AGE42X"])
d = h62
keep = (d.EMPST42.isin([1, 2]) & ((d.SELFCM42 == 2) | ((d.SELFCM42 == -2) & (d.SELFCM31 == 2)))
        & ((d.HRWG42X == -10) | (d.HRWG42X > 0) | ((d.HRWG42X == -2) & ((d.HRWG31X == -10) | (d.HRWG31X > 0))))
        & ((d.HOUR42 > 0) | ((d.HOUR42 == -2) & (d.HOUR31 > 0)))
        & d.RTHLTH42.isin([1, 2, 3, 4, 5]) & (d.TEMPJB42 == 2) & (d.AGE42X > 24))
h62a = d[keep].copy()
h62a["HRLYWAGE"] = np.where(h62a.HRWG42X == -2, h62a.HRWG31X, h62a.HRWG42X)
h62a["HRLYWAGE"] = h62a.HRLYWAGE.replace(-10, 61.98)  # top-coded wage
h62a["HOURS"] = np.select([h62a.HOUR42 > 0, h62a.HOUR42 == -2], [h62a.HOUR42, h62a.HOUR31], np.nan)
h62a["WKLYEARN"] = h62a.HRLYWAGE * h62a.HOURS

# Weighted quartiles of weekly earnings: running sum of weights over sorted earnings ----
h62b = h62a.sort_values("WKLYEARN", kind="stable")
tot_wt, sum_wt = h62b.PERWT02P.sum(), h62b.PERWT02P.cumsum()
h62b["QUARTILE"] = np.select([sum_wt <= tot_wt * 0.25, sum_wt <= tot_wt * 0.5, sum_wt <= tot_wt * 0.75], [1, 2, 3], 4)
h62b = h62b[["RTHLTH42", "WKLYEARN", "QUARTILE", "PERWT02P", "VARSTR", "VARPSU"]]


def freq(s, w=None, title=""):
    f = (s.groupby(s).size() if w is None else w.groupby(s).sum()).rename("Frequency").to_frame()
    f["Percent"] = 100 * f.Frequency / f.Frequency.sum()
    f["Cumulative Frequency"] = f.Frequency.cumsum()
    f["Cumulative Percent"] = 100 * f["Cumulative Frequency"] / f.Frequency.sum()
    show(title, f.reset_index(), "{:,.2f}")


freq(h62b.QUARTILE, h62b.PERWT02P, "QUARTILE (weighted)")
freq(sas_format(h62b.WKLYEARN, WKLYF), h62b.PERWT02P, "WEEKLY EARNINGS (weighted)")
freq(sas_format(h62b.WKLYEARN, WKLYF), None, "WEEKLY EARNINGS")
print(freq_list(h62b.assign(RTHLTH42=sas_format(h62b.RTHLTH42, HLTHF)), ["QUARTILE", "RTHLTH42"])
      .to_string(index=False), "\n")

# PROC TABULATE ... FREQ PERWT02P: PCTN<RTHLTH42> = weighted row percents (FREQ truncates
# the weight to an integer, as SAS does)
fw = np.floor(h62b.PERWT02P)
tab = fw.groupby([sas_format(h62b.QUARTILE, QUARTF), sas_format(h62b.RTHLTH42, HLTHF)]).sum().unstack()
show("PCTN of perceived health status within earnings quartile", (100 * tab.div(tab.sum(axis=1), axis=0))
     .reset_index(), "{:,.2f}")

# PROC SURVEYMEANS ... DOMAIN QUARTILE ---------------------------------------------------
h62b["QUARTILE_F"] = sas_format(h62b.QUARTILE, QUARTF)
show("RTHLTH42 (overall)", surveymeans(h62b, "RTHLTH42", "PERWT02P"))
show("RTHLTH42 by QUARTILE", surveymeans(h62b, "RTHLTH42", "PERWT02P", domain="QUARTILE_F"))
