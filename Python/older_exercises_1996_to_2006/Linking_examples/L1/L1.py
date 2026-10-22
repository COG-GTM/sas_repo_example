# ----------------------------------------------------------------------------
# Program:      c:\meps\prog\Example_L1.sas
#
# Description:  This example shows how to:
#               (1) Identify jobs in first part of 2001
#               (2) Count the numbers of each type of job for each person
#               (3) Merge JOBS and FY files
#               (4) Use SUDAAN to calculate standard errors
#               (5) Use SAS to calculate relative standard errors from SUDAAN output
#
# Input Files:  c:\meps\data\h56.sas7bdat (2001 Jobs)
#               c:\meps\data\h60.sas7bdat (2001 Full-Year Persons)
#
# Python port of SAS/older_exercises_1996_to_2006/Linking_examples/L1/L1.sas
# (the SUDAAN PROC CROSSTAB step is replaced by samplics via meps_utils.svy_freq)
# ----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, crosstab_list, load_meps, svy_freq  # noqa: E402

pd.set_option("display.width", 200)

SUBTYPE = {1: "1 Current Main", 2: "2 Current Miscellaneous", 3: "3 Former Main", 4: "4 Former Miscellaneous",
           5: "5 Last Job Outside Rn", 6: "6 Retirement"}  # fmt: skip
NCURR = {1: "None", 2: " 1", 3: " 2", 4: " 3+"}

# Subset JOBS file to first-round records
H56 = load_meps("h56", columns=["DUPERSID", "PANEL", "RN", "SUBTYPE"])
jobs = H56[((H56["PANEL"] == 5) & (H56["RN"] == 3)) | ((H56["PANEL"] == 6) & (H56["RN"] == 1))]
print(crosstab_list(jobs.assign(SUBTYPE=apply_format(jobs["SUBTYPE"], SUBTYPE)), ["SUBTYPE"]).to_string(index=False))

# Person-level counts of each type of job from first-round JOBS records
#  TOTJOBS = Total # Jobs in Rn 1/3; N1..N6 = # jobs of SUBTYPE 1..6 in Rn 1/3
counts = pd.crosstab(jobs["DUPERSID"], jobs["SUBTYPE"]).reindex(columns=range(1, 7), fill_value=0)
jobsper = counts.rename(columns=lambda k: f"N{k}").assign(TOTJOBS=counts.sum(axis=1)).reset_index()
for c in ["TOTJOBS"] + [f"N{k}" for k in range(1, 7)]:
    print(crosstab_list(jobsper, [c]).T.to_string(header=False))
print(crosstab_list(jobsper, ["TOTJOBS"] + [f"N{k}" for k in range(1, 7)]).to_string(index=False), "\n")

# Combine new person-level file with full-year PUF
H60 = load_meps("h60", columns=["DUPERSID", "PANEL01", "AGE31X", "PERWT01F", "VARSTR01", "VARPSU01"])
curr = H60.merge(jobsper, on="DUPERSID", how="left")

# Count of current main or miscellaneous jobs in Rn 1/3, collapsed into
# categories (starting at 1 as SUDAAN requires); persons without JOBS records
# have a missing count, as in the SAS data step.
curr["NCURR"] = (curr["N1"] + curr["N2"] + 1).clip(upper=4)

# SUBPOPN AGE31X >= 18 (domain, not a subset), excluding missing NCURR like SUDAAN
curr["ADULT"] = np.where((curr["AGE31X"] >= 18) & curr["NCURR"].notna(), 1, 0)
curr["NCURR"] = apply_format(curr["NCURR"].fillna(1), NCURR)
res = svy_freq(curr, "NCURR", domain="ADULT", weight="PERWT01F", strata="VARSTR01", psu="VARPSU01")
y = res[res["ADULT"] == 1].drop(columns="ADULT")
total = pd.DataFrame({"NCURR": ["TOTAL"], "n": [y["n"].sum()], "wgt_freq": [y["wgt_freq"].sum()]})
y = pd.concat([total, y], ignore_index=True)

# Relative standard errors (RSE), flagging any > 0.3
y["rse_wgt"] = y["wgt_freq_se"] / y["wgt_freq"]
y["rse_col"] = y["percent_se"] / y["percent"]
y["flag"] = np.where((y["rse_wgt"] > 0.3) | (y["rse_col"] > 0.3), "*", "")
print("# CURRENT JOBS IN RN 1/3, ADULTS (AGE31X >= 18)")
print(y.rename(columns={"NCURR": "# Curr Jobs", "n": "Sample", "wgt_freq": "Population", "wgt_freq_se": "SE Pop",
                        "percent": "Column %", "percent_se": "SE Col %"}).to_string(index=False))  # fmt: skip
