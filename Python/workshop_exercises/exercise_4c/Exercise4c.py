# -----------------------------------------------------------------------------
# This program illustrates how to pool MEPS data files from different years. It
# highlights one example of a discontinuity that may be encountered when
# working with data from before and after the 2018 CAPI re-design.
#
# The program pools 2017 and 2018 data and calculates for the civilian
# noninstitutionized population:
#  - Percentage of people with Joint Pain / Arthritis (JTPAIN**, ARTHDX)
#  - Average expenditures per person, by Joint Pain status (TOTEXP, TOTSLF)
#
# Notes:
#  - Variables with year-specific names must be renamed before combining files
#    (e.g. 'TOTEXP17' and 'TOTEXP18' renamed to 'totexp')
#
# Input files:
#  - 2017 Full-year consolidated file (h201)
#  - 2018 Full-year consolidated file (h209)
#
# Python port of SAS/workshop_exercises/exercise_4c/Exercise4c.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, crosstab_list, load_meps, svy_freq, svy_mean, svy_total  # noqa: E402

pd.set_option("display.width", 200)

# Formats (PROC FORMAT) -------------------------------------------------------

TOTEXP_FMT = {0: "No Expense"}  # Other = 'Any Expense'
AGECAT_FMT = [(18, 49, "18-49"), (50, 64, "50-64"), (65, np.inf, "65+")]
YES_NO_FMT = {1: "Yes", 2: "No"}


def read_year(name, yy, jtpain):
    cols = ["VARSTR", "VARPSU", f"PERWT{yy}F", "AGELAST", "ARTHDX", jtpain, f"TOTEXP{yy}", f"TOTSLF{yy}"]
    df = load_meps(name, columns=cols).rename(
        columns={f"TOTEXP{yy}": "TOTEXP", f"TOTSLF{yy}": "TOTSLF", jtpain: "JTPAIN"}
    )
    df["PERWTF"] = df[f"PERWT{yy}F"] / 2
    # Create a subpopulation indicator called SPOP and a new variable called
    # JOINT_PAIN based on ARTHDX and JTPAIN31 (2017) / JTPAIN31_M18 (2018)
    spop = (df["AGELAST"] >= 18) & ~((df["ARTHDX"] <= 0) & (df["JTPAIN"] < 0))
    df["SPOP"] = np.where(spop, 1, 2)
    df["JOINT_PAIN"] = np.where(spop, np.where((df["ARTHDX"] == 1) | (df["JTPAIN"] == 1), 1, 2), np.nan)
    return df


MEPS_2017 = read_year("h201", "17", "JTPAIN31")
MEPS_2018 = read_year("h209", "18", "JTPAIN31_M18")

# Concatenate 2017 and 2018 analytic data files
MEPS_1718 = pd.concat([MEPS_2017, MEPS_2018], ignore_index=True)
MEPS_1718["TOTEXP_X"] = MEPS_1718["TOTEXP"]

print("MEPS 2017-18 combined")
print(crosstab_list(MEPS_1718, ["ARTHDX", "JTPAIN", "JOINT_PAIN"]))
print(crosstab_list(MEPS_1718, ["ARTHDX", "JTPAIN", "SPOP"]))
print(crosstab_list(MEPS_1718, ["SPOP"]))
print(crosstab_list(MEPS_1718, ["JOINT_PAIN"]), "\n")

design = dict(weight="PERWTF", strata="VARSTR", psu="VARPSU")
MEPS_1718["JOINT_PAIN"] = apply_format(MEPS_1718["JOINT_PAIN"], YES_NO_FMT)

# Percentage with joint pain (CLASS joint_pain; DOMAIN spop('1')) --------------
print("MEPS 2017-18 combined: joint pain / arthritis, adults (DOMAIN SPOP=1)")
res = svy_freq(MEPS_1718, "JOINT_PAIN", domain="SPOP", **design)
print(res[(res["SPOP"] == 1) & (res["JOINT_PAIN"] != "Missing")].to_string(index=False), "\n")

# Mean / total expenditures (DOMAIN spop('1')*joint_pain) ----------------------
print("MEPS 2017-18 combined: expenditures by joint pain status (DOMAIN SPOP=1 * JOINT_PAIN)")
dom = ["SPOP", "JOINT_PAIN"]
res = svy_mean(MEPS_1718, ["TOTEXP", "TOTSLF"], domain=dom, **design).merge(
    svy_total(MEPS_1718, ["TOTEXP", "TOTSLF"], domain=dom, **design),
    on=dom + ["variable", "n"],
    suffixes=("_mean", "_sum"),
)
print(res[res["SPOP"] == 1].to_string(index=False))
