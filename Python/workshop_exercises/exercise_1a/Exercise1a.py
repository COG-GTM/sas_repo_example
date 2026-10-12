# -----------------------------------------------------------------------------
# Python port of SAS/workshop_exercises/exercise_1a/Exercise1a.sas
# (R reference: R/workshop_exercises/exercise_1a.R)
#
# DESCRIPTION: THIS PROGRAM GENERATES THE FOLLOWING ESTIMATES ON NATIONAL HEALTH
#              CARE EXPENSES, 2016:
#   (1) OVERALL EXPENSES
#   (2) PERCENTAGE OF PERSONS WITH AN EXPENSE
#   (3) MEAN EXPENSE PER PERSON WITH AN EXPENSE, FOR OVERALL, AGE 0-64, AND AGE 65+
#
# INPUT FILE: C:/MEPS/h192.ssp (2016 FULL-YEAR FILE)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np

sys.path.append(str(Path(__file__).resolve().parents[2]))
from meps_utils import meps_path, read_meps, survey_estimate


def main(meps_dir=None):
    h192 = read_meps(meps_path("h192.ssp", meps_dir))

    # Define variables
    h192["has_expTRUE"] = (h192["TOTEXP16"] > 0).astype(int)   # persons with any expense
    h192["has_expFALSE"] = 1 - h192["has_expTRUE"]
    h192["age_cat"] = np.where(h192["AGELAST"] < 65, "<65", "65+")

    # QC
    print(h192.groupby("has_expTRUE")["TOTEXP16"].agg(["min", "max"]))
    print(h192.groupby("age_cat")["AGELAST"].agg(["min", "max"]))

    w = {"weight": "PERWT16F"}
    has_exp = h192["has_expTRUE"] == 1
    results = {
        "mean_exp": survey_estimate(h192, "TOTEXP16", stat="mean", **w),        # mean expense per person
        "total_exp": survey_estimate(h192, "TOTEXP16", stat="total", **w),      # total expenses
        "pct_has_exp": survey_estimate(h192, ["has_expFALSE", "has_expTRUE"], stat="mean", **w),
        "mean_exp_has_exp": survey_estimate(h192, "TOTEXP16", stat="mean", subpop=has_exp, **w),
        "mean_exp_has_exp_by_age": survey_estimate(h192, "TOTEXP16", stat="mean", subpop=has_exp,
                                                   domain="age_cat", **w),
    }
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
