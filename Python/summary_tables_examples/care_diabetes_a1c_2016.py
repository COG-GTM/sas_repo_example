# -----------------------------------------------------------------------------
# Example code to replicate estimates from the MEPS-HC Data Tools summary tables
#
# Accessibility and quality of care: Diabetes Care, 2016
#
# Adults with diabetes: hemoglobin A1c measurement, by race/ethnicity
#  - Number/percent of adults with diabetes (diabetes care survey weight)
#
# Input file: C:/MEPS/h192.ssp (2016 full-year consolidated, SAS XPORT)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from meps_utils import add_dummies, meps_path, read_meps, survey_estimate, wide_estimates

RACE = {
    1: "Hispanic",
    2: "White",
    3: "Black",
    4: "Amer. Indian, AK Native, or mult. races",
    5: "Asian, Hawaiian, or Pacific Islander",
}


def main(meps_dir=None):
    # Load FYC file -------------------------------------------------------------
    fyc = read_meps(meps_path("h192.ssp", meps_dir))

    # Define variables ----------------------------------------------------------
    # Hemoglobin A1c measurement (SAS/R case_when -> numpy.select)
    a1c = fyc["DSA1C53"]
    fyc["diab_a1c"] = np.select(
        [a1c == -1, a1c < 0, a1c == 0, a1c == 96, (a1c > 0) & (a1c < 96)],
        ["Inapplicable", "Don't know/Non-response", "Did not have measurement",
         "Did not have measurement", "Had measurement"],
        default="Missing")

    # Race/ethnicity
    hisp = fyc["RACETHX"] == 1
    white = fyc["RACETHX"] == 2
    black = fyc["RACETHX"] == 3
    native = (fyc["RACETHX"] > 3) & fyc["RACEV1X"].isin([3, 6])
    asian = (fyc["RACETHX"] > 3) & fyc["RACEV1X"].isin([4, 5])
    race = 1 * hisp + 2 * white + 3 * black + 4 * native + 5 * asian
    fyc["race"] = pd.Categorical(race.map(RACE), categories=RACE.values(), ordered=True)

    # Calculate estimates (diabetes care survey weight) --------------------------
    a1c_vars = add_dummies(fyc, "diab_a1c", sorted(fyc["diab_a1c"].unique()))
    number = survey_estimate(fyc, a1c_vars, weight="DIABW16F", stat="total", domain="race")
    percent = survey_estimate(fyc, a1c_vars, weight="DIABW16F", stat="mean", domain="race")

    print("Number of adults with diabetes:")
    print(wide_estimates(number, "race").to_string(index=False), "\n")
    print("Percent of adults with diabetes:")
    print(wide_estimates(percent, "race").to_string(index=False))
    return {"number": number, "percent": percent}


if __name__ == "__main__":
    main()
