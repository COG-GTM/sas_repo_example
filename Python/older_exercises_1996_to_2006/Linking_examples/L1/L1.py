"""Python port of SAS/older_exercises_1996_to_2006/Linking_examples/L1/L1.sas



<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

SUBTYPE = {1: "1 Current Main", 2: "2 Current Miscellaneous", 3: "3 Former Main", 4: "4 Former Miscellaneous",
           5: "5 Last Job Outside Rn", 6: "6 Retirement"}

# Subset the 2001 JOBS file (HC-056) to first-round records ----------------------------
jobs = read_meps("h56")
jobs = jobs[((jobs.PANEL == 5) & (jobs.RN == 3)) | ((jobs.PANEL == 6) & (jobs.RN == 1))].sort_values("DUPERSID",
                                                                                                  kind="stable")
f = sas_format(jobs.SUBTYPE, SUBTYPE).value_counts().sort_index().rename("Frequency").to_frame()
f["Percent"] = 100 * f.Frequency / f.Frequency.sum()
show("PROC FREQ: SUBTYPE", f.reset_index(), "{:,.2f}")

# Person-level counts of each type of job (FIRST./LAST. accumulation -> groupby) ------
jobsper = pd.DataFrame({f"N{k}": (jobs.SUBTYPE == k).groupby(jobs.DUPERSID).sum() for k in range(1, 7)})
jobsper.insert(0, "TOTJOBS", jobs.groupby("DUPERSID").size())
jobsper = jobsper.reset_index()
for v in ["TOTJOBS"] + [f"N{k}" for k in range(1, 7)]:
    print(freq_list(jobsper, [v]).to_string(index=False), "\n")
print(freq_list(jobsper, ["TOTJOBS"] + [f"N{k}" for k in range(1, 7)]).to_string(index=False))

# Combine with the 2001 full-year file (HC-060): MERGE ... BY DUPERSID (outer) ---------
h60 = read_meps("h60", ["DUPERSID", "PANEL01", "AGE31X", "PERWT01F", "VARSTR01", "VARPSU01"])
curr = h60.merge(jobsper, on="DUPERSID", how="outer")
# Current main/miscellaneous jobs in Rn 1/3, collapsed (missing stays missing, as in SAS)
curr["NUMCURR"] = curr.N1 + curr.N2
curr["NCURR"] = (curr.NUMCURR + 1).clip(upper=4)

# SUDAAN PROC CROSSTAB, DESIGN=WR, SUBPOPN AGE31X >= 18: the subpopulation is a domain
# (out-of-subpopulation records stay in the design); records with missing NCURR are
# excluded from the table, so they are placed outside the domain too.
curr["SUB"] = np.where((curr.AGE31X >= 18) & curr.NCURR.notna(), 1, 0)
curr["NCURR_F"] = sas_format(curr.NCURR, {1: "None", 2: " 1", 3: " 2", 4: " 3+"})
design = dict(weight="PERWT01F", strata="VARSTR01", cluster="VARPSU01")
tot = surveyfreq(curr, ["SUB"], **design)
tot = tot[tot.SUB == 1].assign(NCURR_F="TOTAL", RowPercent=100.0, RowStdErr=0.0)
cells = surveyfreq(curr, ["SUB", "NCURR_F"], **design)
x = pd.concat([tot, cells[cells.SUB == 1]], ignore_index=True)
y = pd.DataFrame({"# Curr Jobs": x.NCURR_F, "Sample": x.Frequency, "Population": x.WgtFreq, "SE Pop": x.StdDev})
y["RSE Pop"] = y["SE Pop"] / y.Population
y["Column %"], y["SE Col %"] = x.RowPercent, x.RowStdErr
y["RSE Col %"] = np.where(y["Column %"] > 0, y["SE Col %"] / y["Column %"], np.nan)
y["flag"] = np.where((y["RSE Pop"] > 0.3) | (y["RSE Col %"] > 0.3), "*", "")
show("Current main or miscellaneous jobs in Rn 1/3, persons 18+ (AGE31X)", y, "{:,.3f}")
