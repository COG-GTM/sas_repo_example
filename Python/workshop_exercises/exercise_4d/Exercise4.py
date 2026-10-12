# -----------------------------------------------------------------------------
# Python port of SAS/workshop_exercises/exercise_4d/Exercise4.sas
# (R reference: R/workshop_exercises/exercise_3d.R)
#
# This program illustrates how to pool MEPS data files from different years
# (2017-2019) and use the Pooled Variance Linkage file for variance estimation
# when the pooled years span the 2018 CAPI re-design. It calculates:
#   - Percentage of adults with joint pain / arthritis (any_jtpain)
#   - Average expenditures per person, by joint pain status (totexp, totslf)
#
# Input files: C:/MEPS/h216.dta (2019 FYC), h209.dta (2018 FYC), h201.dta (2017 FYC)
#              C:/MEPS/h36u23.dta (Pooled variance linkage file, 1996-2023)
#
# Note: the SAS/R scripts were written against the 1996-2019 linkage file
# (STRA9619/PSU9619). The current MEPS release only ships the 1996-2023 file, whose
# STRA9623/PSU9623 reproduce the SAS 1996-2019 results exactly.
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))
from meps_utils import meps_path, read_meps, survey_estimate

KEEP = ["DUPERSID", "PANEL", "AGELAST", "perwt", "totslf", "totexp", "any_jtpain"]
YEARS = [("h216.dta", 19, "JTPAIN31_M18"), ("h209.dta", 18, "JTPAIN31_M18"), ("h201.dta", 17, "JTPAIN31")]


def load_year(fname, yy, jtpain, meps_dir):
    df = read_meps(meps_path(fname, meps_dir),
                   columns=["DUPERSID", "PANEL", "AGELAST", "ARTHDX", jtpain,
                            f"PERWT{yy}F", f"TOTSLF{yy}", f"TOTEXP{yy}"])
    df["any_jtpain"] = np.select(
        [(df[jtpain] == 1) | (df["ARTHDX"] == 1), (df[jtpain] < 0) & (df["ARTHDX"] < 0)],
        ["1 Yes", "Missing"], default="2 No")
    adults = df[df["AGELAST"] >= 18]
    print(adults.groupby(["any_jtpain", jtpain, "ARTHDX"]).size())
    df = df.rename(columns={f"PERWT{yy}F": "perwt", f"TOTSLF{yy}": "totslf", f"TOTEXP{yy}": "totexp"})
    return df[KEEP]


def main(meps_dir=None):
    pool = pd.concat([load_year(f, yy, jt, meps_dir) for f, yy, jt in YEARS], ignore_index=True)
    pool["poolwt"] = pool["perwt"] / 3
    linkage = read_meps(meps_path("h36u23.dta", meps_dir), columns=["DUPERSID", "PANEL", "STRA9623", "PSU9623"])
    pool = pool.merge(linkage, on=["DUPERSID", "PANEL"], how="left")

    subpop = (pool["AGELAST"] >= 18) & (pool["any_jtpain"] != "Missing")
    for level in ("1 Yes", "2 No"):
        pool[f"any_jtpain{level}"] = (pool["any_jtpain"] == level).astype(int)

    w = {"weight": "poolwt", "stratum": "STRA9623", "psu": "PSU9623", "subpop": subpop}
    results = {
        "pct_jtpain": survey_estimate(pool, ["any_jtpain1 Yes", "any_jtpain2 No"], stat="mean", **w),
        "mean_exp_by_jtpain": survey_estimate(pool.assign(any_jtpain=pool["any_jtpain"].where(subpop)),
                                              ["totslf", "totexp"], stat="mean", domain="any_jtpain", **w),
    }
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
