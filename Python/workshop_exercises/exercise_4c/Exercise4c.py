# -----------------------------------------------------------------------------
# Python port of SAS/workshop_exercises/exercise_4c/Exercise4c.sas
# (R reference: R/workshop_exercises/exercise_3c.R)
#
# This program illustrates how to pool MEPS data files from different years. It
# highlights one example of a discontinuity that may be encountered when working
# with data from before and after the 2018 CAPI re-design.
#
# The program pools 2017 and 2018 data and calculates:
#   - Percentage of adults with joint pain / arthritis (any_jtpain)
#   - Average expenditures per person, by joint pain status (totexp, totslf)
#
# Notes: JTPAIN31 (2017) was renamed JTPAIN31_M18 in 2018 because its universe changed.
#
# Input files: C:/MEPS/h209.dta (2018 FYC), C:/MEPS/h201.dta (2017 FYC)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))
from meps_utils import meps_path, read_meps, survey_estimate

KEEP = ["DUPERSID", "VARSTR", "VARPSU", "AGELAST", "perwt", "totslf", "totexp", "any_jtpain"]


def load_year(fname, yy, jtpain, meps_dir):
    df = read_meps(meps_path(fname, meps_dir),
                   columns=["DUPERSID", "VARSTR", "VARPSU", "AGELAST", "ARTHDX", jtpain,
                            f"PERWT{yy}F", f"TOTSLF{yy}", f"TOTEXP{yy}"])
    df["any_jtpain"] = np.select(
        [(df[jtpain] == 1) | (df["ARTHDX"] == 1), (df[jtpain] < 0) & (df["ARTHDX"] < 0)],
        ["1 Yes", "Missing"], default="2 No")
    print(df.groupby(["any_jtpain", jtpain, "ARTHDX"]).size())
    df = df.rename(columns={f"PERWT{yy}F": "perwt", f"TOTSLF{yy}": "totslf", f"TOTEXP{yy}": "totexp"})
    return df[KEEP]


def main(meps_dir=None):
    pool = pd.concat([load_year("h209.dta", 18, "JTPAIN31_M18", meps_dir),
                      load_year("h201.dta", 17, "JTPAIN31", meps_dir)], ignore_index=True)
    pool["poolwt"] = pool["perwt"] / 2
    subpop = (pool["AGELAST"] >= 18) & (pool["any_jtpain"] != "Missing")
    for level in ("1 Yes", "2 No"):
        pool[f"any_jtpain{level}"] = (pool["any_jtpain"] == level).astype(int)

    w = {"weight": "poolwt", "subpop": subpop}
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
