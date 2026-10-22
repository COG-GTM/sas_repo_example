# -----------------------------------------------------------------------------
# Python port of SAS/older_exercises_1996_to_2006/Estimation_examples/E1/E1.sas
# (validated against the SAS listing in E1.pdf)
#
# DESCRIPTION: THIS EXAMPLE SHOWS HOW TO COMPUTE PERSON-LEVEL ESTIMATES FOR
#              PERSON-LEVEL HEALTHCARE EXPENDITURES. ESTIMATES INCLUDE: MEANS,
#              PROPORTIONS, TOTALS, AND A REGRESSION OF EXPENDITURES ON SEX.
#
# INPUT FILE:  C:/MEPS/h60.ssp (2001 FULL-YEAR DATA FILE)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np

sys.path.append(str(Path(__file__).resolve().parents[3]))
from meps_utils import meps_path, read_meps, survey_estimate, survey_reg


def main(meps_dir=None):
    h60 = read_meps(meps_path("h60.ssp", meps_dir),
                    columns=["TOTEXP01", "SEX", "AGE01X", "AGE42X", "AGE31X", "PERWT01F", "VARPSU01", "VARSTR01"])
    # AGE: AGE01X, or AGE42X / AGE31X if missing
    h60["AGE"] = h60["AGE01X"].where(h60["AGE01X"] >= 0, h60["AGE42X"].where(
        h60["AGE42X"] >= 0, h60["AGE31X"].where(h60["AGE31X"] >= 0, h60["AGE01X"])))
    # ANY_EXP = 100 rather than 1 so its mean reads as a percentage
    h60["ANY_EXP"] = np.where(h60["TOTEXP01"] == 0, 0, 100)
    h60["AGEF"] = np.select([h60["AGE"].between(0, 64), h60["AGE"].between(65, 90)], ["0-64", "65-90"], None)
    h60["SEXF"] = h60["SEX"].map({1: "MALE", 2: "FEMALE"})
    h60["MALE"] = (h60["SEX"] == 1).astype(int)   # SEX recoded: female = 0, male = 1

    w = {"weight": "PERWT01F", "stratum": "VARSTR01", "psu": "VARPSU01"}
    results = {
        "mean_exp": survey_estimate(h60, "TOTEXP01", stat="mean", **w),
        "mean_exp_by_age": survey_estimate(h60, "TOTEXP01", stat="mean", domain="AGEF", **w),
        "mean_exp_by_sex": survey_estimate(h60, "TOTEXP01", stat="mean", domain="SEXF", **w),
        "reg_exp_on_sex": survey_reg(h60.rename(columns={"MALE": "SEX", "SEX": "SEX_ORIG"}), "TOTEXP01",
                                     numeric=["SEX"], **w),
        "pct_any_exp": survey_estimate(h60, "ANY_EXP", stat="mean", **w),
        "pct_any_exp_by_age": survey_estimate(h60, "ANY_EXP", stat="mean", domain="AGEF", **w),
        "pct_any_exp_by_sex": survey_estimate(h60, "ANY_EXP", stat="mean", domain="SEXF", **w),
        "total_exp": survey_estimate(h60, "TOTEXP01", stat="total", **w),
    }
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
