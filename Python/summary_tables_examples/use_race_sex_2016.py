# -----------------------------------------------------------------------------
# Example code to replicate estimates from the MEPS-HC Data Tools summary tables
#
# Use, expenditures, and population, 2016
#
# Expenditures by race and sex
#  - Number of people
#  - Percent of population with an expense
#  - Total expenditures
#  - Mean expenditure per person
#  - Mean expenditure per person with expense
#  - Median expenditure per person with expense
#
# Input file: C:\MEPS\h192.ssp (2016 full-year consolidated)
#
# Python port of SAS/summary_tables_examples/use_race_sex_2016.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import MEPS_DIR, apply_format, read_meps, svy_mean, svy_quantile, svy_total  # noqa: E402

pd.set_option("display.width", 200)

# Load FYC file (SAS transport file) ------------------------------------------

h192 = read_meps(f"{MEPS_DIR}/h192.ssp")

# Define variables ------------------------------------------------------------
# Race/ethnicity
#  - 1996-2002: race/ethnicity variable based on RACETHNX (see documentation)
#  - 2002-2011: race/ethnicity variable based on RACETHNX and RACEX:
#      hisp   = (RACETHX = 1);
#      white  = (RACETHX = 2);
#      black  = (RACETHX = 3);
#      native = (RACETHX > 3 and RACEV1X in (3,6));
#      asian  = (RACETHX > 3 and RACEV1X in (4,5));
#  - For 2012 and later, use RACETHX and RACEV1X:

MEPS = h192.copy()
MEPS["hisp"] = (MEPS["RACETHX"] == 1).astype(int)
MEPS["white"] = (MEPS["RACETHX"] == 2).astype(int)
MEPS["black"] = (MEPS["RACETHX"] == 3).astype(int)
MEPS["native"] = ((MEPS["RACETHX"] > 3) & MEPS["RACEV1X"].isin([3, 6])).astype(int)
MEPS["asian"] = ((MEPS["RACETHX"] > 3) & MEPS["RACEV1X"].isin([4, 5])).astype(int)
MEPS["race"] = (
    1 * MEPS["hisp"] + 2 * MEPS["white"] + 3 * MEPS["black"] + 4 * MEPS["native"] + 5 * MEPS["asian"]
)
MEPS["person"] = 1  # counter for population totals
MEPS["has_exp"] = (MEPS["TOTEXP16"] > 0).astype(int)  # 1 if person has expense

race = {
    1: "Hispanic",
    2: "White",
    3: "Black",
    4: "Amer. Indian, AK Native, or mult. races",
    5: "Asian, Hawaiian, or Pacific Islander",
}
sex = {1: "Male", 2: "Female"}
MEPS["race"] = apply_format(MEPS["race"], race)
MEPS["SEX"] = apply_format(MEPS["SEX"], sex)

# Calculate estimates using survey procedures ---------------------------------

design = dict(weight="PERWT16F", strata="VARSTR", psu="VARPSU")
dom = ["SEX", "race"]
totals = svy_total(MEPS, ["person", "has_exp", "TOTEXP16"], domain=dom, **design)
means = svy_mean(MEPS, ["person", "has_exp", "TOTEXP16"], domain=dom, **design)


def show(df, var, label):
    return df[df["variable"] == var][dom + ["estimate", "se"]].rename(columns={"estimate": label})


# Number of people
print("Number of people")
print(show(totals, "person", "Number of people").to_string(index=False))

# Percent of population with an expense
print("\nPercent of population with an expense")
print(show(means, "has_exp", "Percent of population with an expense").to_string(index=False))

# Total expenditures
print("\nTotal expenditures")
print(show(totals, "TOTEXP16", "Total expenditures").to_string(index=False))

# Mean expenditure per person
print("\nMean expenditure per person")
print(show(means, "TOTEXP16", "Mean expenditure per person").to_string(index=False))

# Mean and median expenditure per person with expense -------------------------

dom3 = ["has_exp"] + dom
out_mean = svy_mean(MEPS, "TOTEXP16", domain=dom3, **design)
out_median = svy_quantile(MEPS, "TOTEXP16", quantiles=[0.5], domain=dom3, **design)

# Mean expenditure per person with expense
print("\nMean expenditure per person with expense")
print(
    out_mean[out_mean["has_exp"] == 1][dom + ["estimate", "se"]]
    .rename(columns={"estimate": "Mean expenditure per person with expense"})
    .to_string(index=False)
)

# Median expenditure per person with expense
#  Note: Estimates may vary in R, SAS, Stata, and Python, due to different
#        methods of estimating survey quantiles
print("\nMedian expenditure per person with expense")
print(
    out_median[out_median["has_exp"] == 1][dom + ["estimate", "se"]]
    .rename(columns={"estimate": "Median expenditure per person with expense"})
    .to_string(index=False)
)
