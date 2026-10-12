# -----------------------------------------------------------------------------
# Python port of SAS/older_exercises_1996_to_2006/Estimation_examples/E6/E6.sas
# (validated against the SAS listing in E6.pdf)
#
# DESCRIPTION: THIS PROGRAM GENERATES THE FOLLOWING ESTIMATES FOR 2005
#              PERCENTAGE DISTRIBUTION OF HEALTH CARE SPENDING, BY TYPE OF SERVICE,
#              FOR THE U.S. CIVILIAN NONINSTITUTIONALIZED POPULATION, AS REFERENCED IN
#              FIGURE 1 OF THE MEPS STAT BRIEF #193, AND THE PERCENTAGE OF PERSONS WITH
#              AN EXPENSE BY TYPE OF SERVICE (FIGURE 2).
#              NOTE: EXPENSES INCLUDE BOTH FACILITY AND PHYSICIAN EXPENSES.
#
# INPUT FILE:  C:/MEPS/h97.ssp (2005 FULL-YEAR FILE)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[3]))
from meps_utils import meps_path, read_meps, survey_estimate

TOS = ["HOSPITAL_INPATIENT", "OFFICE_BASED", "PRESCRIBED_MEDICINES", "HOSPITAL_OUTPATIENT",
       "DENTAL", "EMERGENCY_ROOM", "HOME_HEALTH", "OTHER"]


def main(meps_dir=None):
    puf = read_meps(meps_path("h97.ssp", meps_dir))
    puf = puf[puf["PERWT05F"] > 0].copy()   # zero-weight records contribute nothing

    # Expenditure variables by type of service
    puf["TOTAL"] = puf["TOTEXP05"]
    puf["HOSPITAL_INPATIENT"] = puf["IPDEXP05"] + puf["IPFEXP05"]
    puf["OFFICE_BASED"] = puf["OBVEXP05"]
    puf["PRESCRIBED_MEDICINES"] = puf["RXEXP05"]
    puf["HOSPITAL_OUTPATIENT"] = puf["OPDEXP05"] + puf["OPFEXP05"]
    puf["DENTAL"] = puf["DVTEXP05"]
    puf["EMERGENCY_ROOM"] = puf["ERDEXP05"] + puf["ERFEXP05"]
    puf["HOME_HEALTH"] = puf["HHAEXP05"] + puf["HHNEXP05"]
    puf["OTHER"] = puf["TOTAL"] - puf[TOS[:-1]].sum(axis=1)

    # Flags (1/0) for persons with an expense, by type of service
    flags = ["X_ANYSVCE"] + [f"X_{s}" for s in TOS]
    for flag, var in zip(flags, ["TOTAL"] + TOS):
        puf[flag] = (puf[var] > 0).astype(int)

    w = {"weight": "PERWT05F"}
    results = {
        "mean_exp": survey_estimate(puf, TOS + ["TOTAL"], stat="mean", **w),
        "total_exp": survey_estimate(puf, TOS + ["TOTAL"], stat="total", **w),
        "pct_of_total": survey_estimate(puf, TOS, stat="ratio", denominator="TOTAL", **w),
        "pct_with_exp": survey_estimate(puf, flags, stat="mean", **w),
        "n_with_exp": survey_estimate(puf, flags, stat="total", **w),
    }
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
