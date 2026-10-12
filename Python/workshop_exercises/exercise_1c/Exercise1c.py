# -----------------------------------------------------------------------------
# Python port of SAS/workshop_exercises/exercise_1c/Exercise1c.sas
# (R reference: R/workshop_exercises/exercise_1b.R)
#
# This program generates the following estimates on national health care expenses
# for the civilian noninstitutionalized population, 2018:
#   - Overall expenses (national totals)
#   - Percentage of persons with an expense
#   - Mean expense per person
#   - Mean/median expense per person with an expense:
#     - Mean expense per person with an expense
#     - Mean expense per person with an expense, by age group
#     - Median expense per person with an expense, by age group
#
# Input file: C:/MEPS/h209.dta (2018 Full-year consolidated file)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np

sys.path.append(str(Path(__file__).resolve().parents[2]))
from meps_utils import meps_path, read_meps, survey_estimate, survey_quantile


def main(meps_dir=None):
    fyc18 = read_meps(meps_path("h209.dta", meps_dir),
                      columns=["AGELAST", "TOTEXP18", "DUPERSID", "VARSTR", "VARPSU", "PERWT18F"])

    fyc18["has_expTRUE"] = (fyc18["TOTEXP18"] > 0).astype(int)   # persons with any expense
    fyc18["has_expFALSE"] = 1 - fyc18["has_expTRUE"]
    fyc18["age_cat"] = np.where(fyc18["AGELAST"] < 65, "<65", "65+")

    # QC
    print(fyc18.groupby(["has_expTRUE", "age_cat"]).size())
    print(fyc18.groupby("has_expTRUE")["TOTEXP18"].agg(["min", "max"]))
    print(fyc18.groupby("age_cat")["AGELAST"].agg(["min", "max"]))

    w = {"weight": "PERWT18F"}
    has_exp = fyc18["has_expTRUE"] == 1
    results = {
        "total_exp": survey_estimate(fyc18, "TOTEXP18", stat="total", **w),
        "pct_has_exp": survey_estimate(fyc18, ["has_expFALSE", "has_expTRUE"], stat="mean", **w),
        "mean_exp": survey_estimate(fyc18, "TOTEXP18", stat="mean", **w),
        "mean_exp_has_exp": survey_estimate(fyc18, "TOTEXP18", stat="mean", subpop=has_exp, **w),
        "mean_exp_has_exp_by_age": survey_estimate(fyc18, "TOTEXP18", stat="mean", subpop=has_exp,
                                                   domain="age_cat", **w),
        "median_exp_has_exp_by_age": survey_quantile(fyc18, "TOTEXP18", quantile=0.5, subpop=has_exp,
                                                     domain="age_cat", **w),
    }
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
