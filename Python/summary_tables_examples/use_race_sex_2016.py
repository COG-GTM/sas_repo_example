"""Python port of SAS/summary_tables_examples/use_race_sex_2016.sas

Example code to replicate estimates from the MEPS-HC Data Tools summary tables

Use, expenditures, and population, 2016

Expenditures by race and sex
 - Number of people
 - Percent of population with an expense
 - Total expenditures
 - Mean expenditure per person
 - Mean expenditure per person with expense
 - Median expenditure per person with expense

Input file: <MEPS_DIR>/h192.ssp (2016 full-year consolidated)

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path


sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# Load FYC file (SAS: PROC XCOPY) -----------------------------------------------
h192 = read_meps("h192")

# Define variables ----------------------------------------------------------------
#  - 1996-2002: race/ethnicity variable based on RACETHNX (see documentation)
#  - 2002-2011: race/ethnicity variable based on RACETHNX and RACEX:
#      hisp   = (RACETHNX == 1)
#      white  = (RACETHNX == 4) & (RACEX == 1)
#      black  = (RACETHNX == 2)
#      native = (RACETHNX >= 3) & RACEX.isin([3, 6])
#      asian  = (RACETHNX >= 3) & RACEX.isin([4, 5])
#  - 2012 and later: use RACETHX and RACEV1X
MEPS = h192.copy()
MEPS["hisp"] = (MEPS.RACETHX == 1).astype(int)
MEPS["white"] = (MEPS.RACETHX == 2).astype(int)
MEPS["black"] = (MEPS.RACETHX == 3).astype(int)
MEPS["native"] = ((MEPS.RACETHX > 3) & MEPS.RACEV1X.isin([3, 6])).astype(int)
MEPS["asian"] = ((MEPS.RACETHX > 3) & MEPS.RACEV1X.isin([4, 5])).astype(int)
MEPS["race"] = 1 * MEPS.hisp + 2 * MEPS.white + 3 * MEPS.black + 4 * MEPS.native + 5 * MEPS.asian
MEPS["person"] = 1                                # counter for population totals
MEPS["has_exp"] = (MEPS.TOTEXP16 > 0).astype(int)  # 1 if person has expense

# PROC FORMAT
race = {1: "Hispanic", 2: "White", 3: "Black", 4: "Amer. Indian, AK Native, or mult. races",
        5: "Asian, Hawaiian, or Pacific Islander"}
sex = {1: "Male", 2: "Female"}
MEPS["SEX"] = sas_format(MEPS.SEX, sex)
MEPS["race"] = sas_format(MEPS.race, race)

# Calculate estimates using survey procedures -------------------------------------
out = surveymeans(MEPS, ["person", "has_exp", "TOTEXP16"], weight="PERWT16F", strata="VARSTR", cluster="VARPSU",
                  domain=["SEX", "race"], missing=True)
show("Number of people", out[out.VarName == "person"][["SEX", "race", "Sum", "StdDev"]])
show("Percent of people with an expense", out[out.VarName == "has_exp"][["SEX", "race", "Mean", "StdErr"]])
show("Total expenditures", out[out.VarName == "TOTEXP16"][["SEX", "race", "Sum", "StdDev"]])
show("Mean expenditure per person", out[out.VarName == "TOTEXP16"][["SEX", "race", "Mean", "StdErr"]])

# Mean and median expenditure per person with an expense (DOMAIN has_exp*sex*race)
out_mean = surveymeans(MEPS, "TOTEXP16", weight="PERWT16F", strata="VARSTR", cluster="VARPSU",
                       domain=["has_exp", "SEX", "race"], missing=True)
out_median = surveyquantile(MEPS, "TOTEXP16", weight="PERWT16F", strata="VARSTR", cluster="VARPSU",
                            domain=["has_exp", "SEX", "race"], q=0.5, missing=True)
show("Mean expenditure per person with expense", out_mean[out_mean.has_exp == 1][["SEX", "race", "Mean", "StdErr"]])
show("Median expenditure per person with expense",
     out_median[out_median.has_exp == 1][["SEX", "race", "Estimate", "StdErr"]])
