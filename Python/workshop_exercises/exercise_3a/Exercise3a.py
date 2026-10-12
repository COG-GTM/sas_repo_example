# -----------------------------------------------------------------------------
# Python port of SAS/workshop_exercises/exercise_3a/Exercise3a.sas
# (no R counterpart; validated against Exercise3a_OUTPUT.TXT)
#
# DESCRIPTION: THIS PROGRAM ILLUSTRATES HOW TO IDENTIFY PERSONS WITH A CONDITION AND
#              CALCULATE ESTIMATES ON USE AND EXPENDITURES FOR PERSONS WITH THE CONDITION.
#              THE CONDITION USED IN THIS EXERCISE IS DIABETES (CCS CODE=049 OR 050).
#
# INPUT FILES: 1) C:/MEPS/h180.ssp (2015 CONDITION PUF DATA)
#              2) C:/MEPS/h181.ssp (2015 FY PUF DATA)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))
from meps_utils import meps_path, read_meps, survey_estimate

SEX = {1: "MALE", 2: "FEMALE"}
VARS = ["TOTEXP15", "TOTSLF15", "OBTOTV15"]


def main(meps_dir=None):
    cond = read_meps(meps_path("h180.ssp", meps_dir), columns=["DUPERSID", "CONDIDX", "CCCODEX"])
    fy = read_meps(meps_path("h181.ssp", meps_dir),
                   columns=["DUPERSID", "SEX", "VARSTR", "VARPSU", "PERWT15F"] + VARS)

    # 1) Pull out conditions with diabetes (CCS code '049', '050')
    diab = cond[cond["CCCODEX"].isin(["049", "050"])]
    print(diab["CCCODEX"].value_counts())

    # 2-3) Flag persons who reported diabetes in the FY data
    fy["DIABPERS"] = fy["DUPERSID"].isin(diab["DUPERSID"]).map({True: 1, False: 2})
    fy["SEX"] = fy["SEX"].map(SEX)
    print(fy.groupby(["DIABPERS", "SEX"]).size())
    print(fy.groupby(["DIABPERS", "SEX"])["PERWT15F"].sum())

    # 4) Estimates on use and expenditures for persons who reported diabetes (domain DIABPERS=1)
    w = {"weight": "PERWT15F"}
    diab_pers = fy["DIABPERS"] == 1
    results = {
        "total_diab": survey_estimate(fy, VARS, stat="total", subpop=diab_pers, **w),
        "mean_diab": survey_estimate(fy, VARS, stat="mean", subpop=diab_pers, **w),
        "total_diab_by_sex": survey_estimate(fy, VARS, stat="total", subpop=diab_pers, domain="SEX", **w),
        "mean_diab_by_sex": survey_estimate(fy, VARS, stat="mean", subpop=diab_pers, domain="SEX", **w),
    }
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
