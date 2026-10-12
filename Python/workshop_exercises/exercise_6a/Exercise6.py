# -----------------------------------------------------------------------------
# Python port of SAS/workshop_exercises/exercise_6a/Exercise6.sas
# (R reference: R/workshop_exercises/exercise_4a.R)
#
# This program includes a regression example for persons receiving a flu shot in
# the last 12 months for the U.S. civilian non-institutionalized population, including:
#   - Percentage of adults (18+) receiving a flu shot
#   - Logistic regression: to identify demographic factors associated with receiving
#     a flu shot (age, sex, race/ethnicity, insurance coverage)
#
# Input file: C:/MEPS/h209.dta (2018 full-year consolidated file)
# Weight: SAQWT18F (Self-Administered Questionnaire weight)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np

sys.path.append(str(Path(__file__).resolve().parents[2]))
from meps_utils import meps_path, read_meps, survey_estimate, survey_glm


def main(meps_dir=None):
    fyc18 = read_meps(meps_path("h209.dta", meps_dir),
                      columns=["DUPERSID", "VARPSU", "VARSTR", "SAQWT18F", "ADFLST42", "AGELAST",
                               "SEX", "RACETHX", "INSCOV18"])
    print(fyc18.loc[fyc18["SAQWT18F"] > 0, "ADFLST42"].value_counts().sort_index())

    # flu_shot: 1 = yes, 0 = no (negative codes kept and excluded via the subpopulation)
    fyc18["flu_shot"] = np.select([fyc18["ADFLST42"] == 1, fyc18["ADFLST42"] == 2], [1, 0],
                                  default=fyc18["ADFLST42"])
    subpop = fyc18["ADFLST42"] >= 0
    print(fyc18.groupby(["flu_shot", "ADFLST42"]).size())

    w = {"weight": "SAQWT18F", "subpop": subpop}
    results = {
        "mean_flu_shot": survey_estimate(fyc18, "flu_shot", stat="mean", **w),
        "glm_flu_shot": survey_glm(fyc18, "flu_shot", numeric=["AGELAST"],
                                   factors=["SEX", "RACETHX", "INSCOV18"], **w),
    }
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
