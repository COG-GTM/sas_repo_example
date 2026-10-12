# -----------------------------------------------------------------------------
# Python port of SAS/workshop_exercises/exercise_1b/Exercise1b.sas
# (no R counterpart; validated against Exercise1b_OUTPUT.TXT)
#
# DESCRIPTION: THIS PROGRAM GENERATES THE FOLLOWING ESTIMATES ON NATIONAL HEALTH
#              CARE EXPENSES BY TYPE OF SERVICE, 2015:
#   (1) PERCENTAGE DISTRIBUTION OF EXPENSES BY TYPE OF SERVICE
#   (2) PERCENTAGE OF PERSONS WITH AN EXPENSE, BY TYPE OF SERVICE
#   (3) MEAN EXPENSE PER PERSON WITH AN EXPENSE, BY TYPE OF SERVICE
#
# NOTE: EXPENSES INCLUDE BOTH FACILITY AND PHYSICIAN EXPENSES.
# INPUT FILE: C:/MEPS/h181.ssp (2015 FULL-YEAR FILE)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np

sys.path.append(str(Path(__file__).resolve().parents[2]))
from meps_utils import meps_path, read_meps, survey_estimate

AGECAT = {1: "0-64", 2: "65+"}
SERVICES = ["HOSPITAL_INPATIENT", "AMBULATORY", "PRESCRIBED_MEDICINES", "DENTAL", "HOME_HEALTH_OTHER"]


def main(meps_dir=None):
    puf = read_meps(meps_path("h181.ssp", meps_dir))

    # Define expenditure variables by type of service
    puf["TOTAL"] = puf["TOTEXP15"]
    puf["HOSPITAL_INPATIENT"] = puf["IPDEXP15"] + puf["IPFEXP15"]
    puf["AMBULATORY"] = puf[["OBVEXP15", "OPDEXP15", "OPFEXP15", "ERDEXP15", "ERFEXP15"]].sum(axis=1)
    puf["PRESCRIBED_MEDICINES"] = puf["RXEXP15"]
    puf["DENTAL"] = puf["DVTEXP15"]
    puf["HOME_HEALTH_OTHER"] = puf[["HHAEXP15", "HHNEXP15", "OTHEXP15", "VISEXP15"]].sum(axis=1)

    # QC: the sum of expenditures by type of service equals the total
    diff = puf["TOTAL"] - puf[SERVICES].sum(axis=1)
    print("Max |DIFF|:", diff.abs().max())

    # Flags (1/0) for persons with an expense, by type of service
    puf["X_ANYSVCE"] = (puf["TOTAL"] > 0).astype(int)
    for s in SERVICES:
        puf[f"X_{s}"] = (puf[s] > 0).astype(int)

    # Summary age from end-of-year, round 4/2, and round 3/1 ages
    age = puf["AGE15X"].where(puf["AGE15X"] >= 0,
                              puf["AGE42X"].where(puf["AGE42X"] >= 0,
                                                  puf["AGE31X"].where(puf["AGE31X"] >= 0)))
    puf["AGECAT"] = np.select([age.between(0, 64), age > 64], [AGECAT[1], AGECAT[2]], default=None)
    print(puf.groupby("AGECAT", dropna=False)["AGE15X"].agg(["min", "max", "size"]))

    w = {"weight": "PERWT15F"}
    flags = ["X_ANYSVCE"] + [f"X_{s}" for s in SERVICES]
    any_exp = puf["X_ANYSVCE"] == 1
    results = {
        # (1) Percentage distribution of expenses by type of service
        "total_by_service": survey_estimate(puf, SERVICES + ["TOTAL"], stat="total", **w),
        "share_of_total": survey_estimate(puf, SERVICES, stat="ratio", denominator="TOTAL", **w),
        # (2) Percentage of persons with an expense, by type of service
        "pct_with_expense": survey_estimate(puf, flags, stat="mean", **w),
        "persons_with_expense": survey_estimate(puf, flags, stat="total", **w),
        # (3) Mean expense per person (overall, by age, and among persons with an expense)
        "mean_total": survey_estimate(puf, "TOTAL", stat="mean", **w),
        "mean_total_by_age": survey_estimate(puf, "TOTAL", stat="mean", domain="AGECAT", **w),
        "mean_total_has_exp": survey_estimate(puf, "TOTAL", stat="mean", subpop=any_exp, **w),
        "mean_total_has_exp_by_age": survey_estimate(puf, "TOTAL", stat="mean", subpop=any_exp,
                                                     domain="AGECAT", **w),
    }
    for s in SERVICES:
        has = puf[f"X_{s}"] == 1
        results[f"mean_{s}"] = survey_estimate(puf, s, stat="mean", **w)
        results[f"mean_{s}_has_exp"] = survey_estimate(puf, s, stat="mean", subpop=has, **w)
        results[f"mean_{s}_has_exp_by_age"] = survey_estimate(puf, s, stat="mean", subpop=has,
                                                              domain="AGECAT", **w)
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
