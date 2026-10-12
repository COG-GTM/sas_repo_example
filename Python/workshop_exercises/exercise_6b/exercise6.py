# -----------------------------------------------------------------------------
# Python port of SAS/workshop_exercises/exercise_6b/exercise6.sas
# (R reference: R/workshop_exercises/exercise_4b.R)
#
# This program includes an example of 3 logistic regression models, each with a
# different dependent variable: delayed medical care, dental care, and prescribed
# medicines due to COVID-19, for the U.S. civilian non-institutionalized population.
#   - Percentage of people delaying each type of care
#   - Logistic regressions on age, sex, race/ethnicity, insurance coverage and region
#
# Input file: C:/MEPS/h224.dta (2020 full-year consolidated file)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np

sys.path.append(str(Path(__file__).resolve().parents[2]))
from meps_utils import meps_path, read_meps, survey_estimate, survey_glm

DELAY = {"CARE": "CVDLAYCA53", "DENTAL": "CVDLAYDN53", "PMED": "CVDLAYPM53"}


def main(meps_dir=None):
    fyc20 = read_meps(meps_path("h224.dta", meps_dir),
                      columns=["DUPERSID", "VARPSU", "VARSTR", "PERWT20F", "AGELAST", "SEX", "RACETHX",
                               "INSCOV20", "REGION53", *DELAY.values()])
    subpops = {}
    for kind, src in DELAY.items():
        v = fyc20[src].astype(float)
        fyc20[f"covid_delay_{kind}"] = np.select([v == 1, v == 2], [1, 0], default=v)
        subpops[kind] = v >= 0
        print(fyc20.groupby([f"covid_delay_{kind}", src]).size())

    results = {}
    for kind in DELAY:
        results[f"mean_delay_{kind.lower()}"] = survey_estimate(
            fyc20, f"covid_delay_{kind}", weight="PERWT20F", stat="mean", subpop=subpops[kind])
    for kind in DELAY:
        results[f"glm_delay_{kind.lower()}"] = survey_glm(
            fyc20, f"covid_delay_{kind}", weight="PERWT20F", numeric=["AGELAST"],
            factors=["SEX", "RACETHX", "INSCOV20", "REGION53"], subpop=subpops[kind])
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
