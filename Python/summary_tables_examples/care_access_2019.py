# -----------------------------------------------------------------------------
# Example code to replicate estimates from the MEPS-HC Data Tools summary tables
#
# Accessibility and quality of care: Access to Care, 2019
#
# Did not receive treatment because couldn't afford it
#  - Number/percent of people
#  - By poverty status
#
# Input file: C:/MEPS/h216.sas7bdat (2019 full-year consolidated)
#
# Python port of SAS/summary_tables_examples/care_access_2019.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import MEPS_DIR, add_domain, crosstab_list, read_meps, svy_mean, svy_total  # noqa: E402

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)

# Load FYC file ---------------------------------------------------------------

h216 = read_meps(f"{MEPS_DIR}/h216.sas7bdat")


# Define variables ------------------------------------------------------------

MEPS = h216.copy()

# Didn't receive care because couldn't afford it
MEPS["afford_MD"] = (MEPS["AFRDCA42"] == 1).astype(int)  # medical care
MEPS["afford_DN"] = (MEPS["AFRDDN42"] == 1).astype(int)  # dental care
MEPS["afford_PM"] = (MEPS["AFRDPM42"] == 1).astype(int)  # prescribed medicines
MEPS["afford_ANY"] = (MEPS[["afford_MD", "afford_DN", "afford_PM"]].sum(axis=1) > 0).astype(int)  # any care

# Define domain and adjust weights so observations aren't dropped
#  - includes persons eligible to receive the 'access to care' supplement
MEPS = add_domain(MEPS, MEPS["ACCELI42"] == 1, weight="PERWT19F")

poverty = {
    1: "1 Negative or poor",
    2: "2 Near-poor",
    3: "3 Low income",
    4: "4 Middle Income",
    5: "5 High Income",
}
MEPS["POVCAT19"] = MEPS["POVCAT19"].map(poverty)


# QC new variables
for cols in (
    ["AFRDCA42", "afford_MD"],
    ["AFRDDN42", "afford_DN"],
    ["AFRDPM42", "afford_PM"],
    ["afford_MD", "afford_DN", "afford_PM", "afford_ANY"],
):
    print(crosstab_list(MEPS, cols), "\n")


# Calculate estimates using survey procedures ---------------------------------

# Did not receive treatment because couldn't afford it, by poverty status
#  sum  = Number of people
#  mean = Percent of people
outcomes = ["afford_ANY", "afford_MD", "afford_DN", "afford_PM"]
design = dict(weight="PERWT19F", strata="VARSTR", psu="VARPSU", domain=["domain", "POVCAT19"])

totals = svy_total(MEPS, outcomes, **design)
means = svy_mean(MEPS, outcomes, **design)

print("Number of people (sum):")
print(totals[totals["domain"] == 1].drop(columns="domain").to_string(index=False))
print("\nPercent of people (mean):")
print(means[means["domain"] == 1].drop(columns="domain").to_string(index=False))
