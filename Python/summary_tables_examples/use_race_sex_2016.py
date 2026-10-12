# -----------------------------------------------------------------------------
# Example code to replicate estimates from the MEPS-HC Data Tools summary tables
#
# Use, expenditures, and population, 2016
#
# Expenditures by race and sex
#  - Number of people
#  - Percent of population with an expense
#  - Mean expense per person
#  - Mean/median expense per person with an expense
#
# Input file: C:/MEPS/h192.ssp (2016 full-year consolidated, SAS XPORT)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from meps_utils import meps_path, read_meps, survey_estimate, survey_quantile

RACE = {
    1: "Hispanic",
    2: "White",
    3: "Black",
    4: "Amer. Indian, AK Native, or mult. races",
    5: "Asian, Hawaiian, or Pacific Islander",
}
SEX = {1: "Male", 2: "Female"}


def main(meps_dir=None):
    # Load FYC file -------------------------------------------------------------
    fyc = read_meps(meps_path("h192.ssp", meps_dir))

    # Define variables ----------------------------------------------------------
    hisp = fyc["RACETHX"] == 1
    white = fyc["RACETHX"] == 2
    black = fyc["RACETHX"] == 3
    native = (fyc["RACETHX"] > 3) & fyc["RACEV1X"].isin([3, 6])
    asian = (fyc["RACETHX"] > 3) & fyc["RACEV1X"].isin([4, 5])
    race = 1 * hisp + 2 * white + 3 * black + 4 * native + 5 * asian
    fyc["race"] = pd.Categorical(race.map(RACE), categories=RACE.values(), ordered=True)
    fyc["sex"] = pd.Categorical(fyc["SEX"].map(SEX), categories=SEX.values(), ordered=True)

    fyc["persons"] = 1                                # counter for population totals
    fyc["has_exp"] = (fyc["TOTEXP16"] > 0).astype(int)  # 1 if person has an expense

    # QC
    print(fyc.groupby(["persons", "has_exp"])["TOTEXP16"].agg(["min", "mean", "max"]), "\n")

    # Calculate estimates -------------------------------------------------------
    d = {"weight": "PERWT16F", "domain": ["sex", "race"]}
    has_exp = fyc["has_exp"] == 1
    results = {
        "n_people": survey_estimate(fyc, "persons", stat="total", **d),
        "pct_has_exp": survey_estimate(fyc, "has_exp", stat="mean", **d),
        "n_people_2": survey_estimate(fyc, "persons", stat="total", **d),  # repeated in R script
        "mean_exp": survey_estimate(fyc, "TOTEXP16", stat="mean", **d),
        "mean_exp_has_exp": survey_estimate(fyc, "TOTEXP16", stat="mean", subpop=has_exp, **d),
        "median_exp_has_exp": survey_quantile(fyc, "TOTEXP16", quantile=0.5, subpop=has_exp, **d),
    }

    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
