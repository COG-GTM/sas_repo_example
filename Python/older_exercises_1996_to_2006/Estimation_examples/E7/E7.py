# -----------------------------------------------------------------------------
# Python port of SAS/older_exercises_1996_to_2006/Estimation_examples/E7/E7.sas
# (validated against the SAS listing in E7.pdf)
#
# DESCRIPTION: PERCENTAGE OF PERSONS WHO EVER HAD A COLONOSCOPY / SIGMOIDOSCOPY
#              (BOWEL53), BY AGE, RACE/ETHNICITY AND EDUCATION, 2005 (PROC SURVEYFREQ).
#
# INPUT FILE:  C:/MEPS/h97.ssp (2005 FULL-YEAR FILE)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np

sys.path.append(str(Path(__file__).resolve().parents[3]))
from meps_utils import meps_path, read_meps, survey_freq

AGECAT = {1: "1 AGE CATEGORY 0-49", 2: "2 AGE CATEGORY 50-64", 3: "3 AGE CATEGORY 65+"}
AGE50P = {1: "1 AGE CATEGORY 0-49", 2: "2 AGE CATEGORY 50+"}
EDUCF = {1: "1 LESS THAN HIGH SCHOOL", 2: "2 HIGH SCHOOL GRAD", 3: "3 AT LEAST SOME COLLEGE",
         4: "4 UNKNOWN (-7/-8/-9)", 5: "5 YOUNGER THAN 16"}
RACEF = {1: "1 HISPANIC", 2: "2 WHITE NON-HISPANIC", 3: "3 BLACK NON-HISPANIC", 4: "4 ASIAN NON-HISPANIC",
         5: "5 OTHER NON-HISPANIC"}


def yesno(v):
    """SAS format YESNO: LOW-<0='<0', 0='0', 1='1, YES', 2='2, NO'."""
    return np.select([v < 0, v == 0, v == 1, v == 2], ["<0", "0", "1, YES", "2, NO"], default=None)


def main(meps_dir=None):
    puf = read_meps(meps_path("h97.ssp", meps_dir),
                    columns=["DUPERSID", "VARSTR", "VARPSU", "AGE05X", "AGE53X", "AGE42X", "AGE31X",
                             "RACEX", "HISPANX", "EDUCYR", "BOWEL53", "WHNBWL53", "PERWT05F"])
    age = puf["AGE05X"].where(puf["AGE05X"] >= 0, puf["AGE53X"].where(puf["AGE53X"] >= 0, puf["AGE42X"].where(
        puf["AGE42X"] >= 0, puf["AGE31X"].where(puf["AGE31X"] >= 0, -1))))
    puf["AGECAT"] = np.select([age.between(0, 49), age.between(50, 64), age >= 65], [1, 2, 3], -1)
    puf["AGE50PLUS"] = np.select([age.between(0, 49), age >= 50], [1, 2], -1)
    hisp = puf["HISPANX"] == 1
    raceth = np.select([hisp, (puf["HISPANX"] == 2) & (puf["RACEX"] == 1), (puf["HISPANX"] == 2) & (puf["RACEX"] == 2),
                        (puf["HISPANX"] == 2) & (puf["RACEX"] == 3), (puf["HISPANX"] == 2) & (puf["RACEX"] == 4),
                        (puf["HISPANX"] == 2) & puf["RACEX"].isin([5, 6])],
                       [1, 2, 3, 4, 5, 6], 0)
    puf["NEWRACE"] = np.select([raceth == 1, raceth == 2, raceth == 3, raceth == 5, np.isin(raceth, [4, 6])],
                               [1, 2, 3, 4, 5], 0)
    puf["HIGHEDUC"] = np.select([age < 16, puf["EDUCYR"] < 0, puf["EDUCYR"] < 12, puf["EDUCYR"] == 12,
                                 puf["EDUCYR"] > 12], [5, 4, 1, 2, 3], -1)

    # formatted values, as PROC SURVEYFREQ groups by them (unformatted codes -> excluded)
    puf["BOWEL53"] = yesno(puf["BOWEL53"])
    puf["AGECAT"] = puf["AGECAT"].map(AGECAT)
    puf["AGE50PLUS"] = puf["AGE50PLUS"].map(AGE50P)
    puf["NEWRACE"] = puf["NEWRACE"].map(RACEF)
    puf["HIGHEDUC"] = puf["HIGHEDUC"].map(EDUCF)

    w = {"weight": "PERWT05F", "col": "BOWEL53"}
    tables = {
        "age": survey_freq(puf, row="AGECAT", **w),
        "age50": survey_freq(puf, row="AGE50PLUS", **w),
        "age50_race": survey_freq(puf, row="NEWRACE", layer="AGE50PLUS", **w),
        "age50_educ": survey_freq(puf, row="HIGHEDUC", layer="AGE50PLUS", **w),
    }
    results = {f"{name}_{k}": v for name, t in tables.items() for k, v in t.items()}
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
