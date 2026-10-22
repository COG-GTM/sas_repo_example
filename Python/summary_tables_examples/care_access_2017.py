# -----------------------------------------------------------------------------
# Example code to replicate estimates from the MEPS-HC Data Tools summary tables
#
# Accessibility and quality of care: Access to Care, 2017
#
# Reasons for difficulty receiving needed care
#  - Number/percent of people
#  - By poverty status
#
# Input file: C:/MEPS/h201.sas7bdat (2017 full-year consolidated)
#
# Python port of SAS/summary_tables_examples/care_access_2017.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import MEPS_DIR, add_domain, crosstab_list, read_meps, svy_mean, svy_total  # noqa: E402

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)

# Load FYC file ---------------------------------------------------------------

h201 = read_meps(f"{MEPS_DIR}/h201.sas7bdat")

# Define variables ------------------------------------------------------------

MEPS = h201.copy()


def flag(cond) -> pd.Series:
    return cond.astype(int)


# Reasons for difficulty receiving needed care
#  any delay / unable to receive needed care
for sfx, pre in (("MD", "MD"), ("DN", "DN"), ("PM", "PM")):
    dl, un = MEPS[f"{pre}DLRS42"], MEPS[f"{pre}UNRS42"]
    MEPS[f"delay_{sfx}"] = flag((MEPS[f"{pre}UNAB42"] == 1) | (MEPS[f"{pre}DLAY42"] == 1))
    # Among people unable or delayed, how many...
    # ...couldn't afford
    MEPS[f"afford_{sfx}"] = flag((dl == 1) | (un == 1))
    # ...had insurance problems
    MEPS[f"insure_{sfx}"] = flag(dl.isin([2, 3]) | un.isin([2, 3]))
    # ...other
    MEPS[f"other_{sfx}"] = flag((dl > 3) | (un > 3))

for v in ("delay", "afford", "insure", "other"):
    MEPS[f"{v}_ANY"] = flag(MEPS[[f"{v}_MD", f"{v}_DN", f"{v}_PM"]].sum(axis=1) > 0)

# Define domain and adjust weights so observations are not dropped
#  - domain includes persons eligible to receive the 'access to care'
#    supplement and who experienced difficulty receiving needed care
MEPS = add_domain(MEPS, (MEPS["ACCELI42"] == 1) & (MEPS["delay_ANY"] == 1), weight="PERWT17F")

poverty = {
    1: "1 Negative or poor",
    2: "2 Near-poor",
    3: "3 Low income",
    4: "4 Middle Income",
    5: "5 High Income",
}
MEPS["POVCAT17"] = MEPS["POVCAT17"].map(poverty)

# QC new variables (PROC FREQ ... / LIST) -------------------------------------

for cols in (
    ["delay_MD", "MDUNAB42", "MDDLAY42"],
    ["delay_DN", "DNUNAB42", "DNDLAY42"],
    ["delay_PM", "PMUNAB42", "PMDLAY42"],
    ["delay_ANY", "delay_MD", "delay_DN", "delay_PM"],
):
    print(crosstab_list(MEPS, cols), "\n")

# Calculate estimates using survey procedures ---------------------------------
# Reasons for difficulty receiving any needed care, by poverty status
#  sum  = Number of people
#  mean = Percent of people

outcomes = ["afford_ANY", "insure_ANY", "other_ANY"]
design = dict(weight="PERWT17F", strata="VARSTR", psu="VARPSU", domain=["domain", "POVCAT17"])

totals = svy_total(MEPS, outcomes, **design)
means = svy_mean(MEPS, outcomes, **design)

print("Number of people (sum):")
print(totals[totals["domain"] == 1].drop(columns="domain").to_string(index=False))
print("\nPercent of people (mean):")
print(means[means["domain"] == 1].drop(columns="domain").to_string(index=False))
