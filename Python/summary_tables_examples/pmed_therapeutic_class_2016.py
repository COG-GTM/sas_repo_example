# -----------------------------------------------------------------------------
# Example code to replicate estimates from the MEPS-HC Data Tools summary tables
#
# Prescribed drugs, 2016
#
# Purchases and expenditures by Multum therapeutic class name (TC1)
#  - Number of people with purchase
#  - Total purchases
#  - Total expenditures
#
# Input file: C:\MEPS\h188a.ssp (2016 RX event file)
#
# Python port of SAS/summary_tables_examples/pmed_therapeutic_class_2016.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import MEPS_DIR, apply_format, read_meps, svy_total  # noqa: E402

pd.set_option("display.width", 200)

# Define formats for therapeutic classes --------------------------------------

TC1name = {
    -9: "Not ascertained",
    -1: "Inapplicable",
    1: "Anti-infectives",
    19: "Antihyperlipidemic agents",
    20: "Antineoplastics",
    28: "Biologicals",
    40: "Cardiovascular agents",
    57: "Central nervous system agents",
    81: "Coagulation modifiers",
    87: "Gastrointestinal agents",
    97: "Hormones/hormone modifiers",
    105: "Miscellaneous agents",
    113: "Genitourinary tract agents",
    115: "Nutritional products",
    122: "Respiratory agents",
    133: "Topical agents",
    218: "Alternative medicines",
    242: "Psychotherapeutic agents",
    254: "Immunologic agents",
    358: "Metabolic agents",
}

# Load datasets ---------------------------------------------------------------
# For 1996-2013, need to merge RX event file with Multum Lexicon Addendum
#  file to get therapeutic class categories and generic drug names

# Load RX file
h188a = read_meps(f"{MEPS_DIR}/h188a.ssp")

# Aggregate to person-level (PROC SORT + PROC MEANS BY ...) -------------------

by = ["DUPERSID", "VARSTR", "VARPSU", "PERWT16F", "TC1"]
TC1pers = (
    h188a.groupby(by)
    .agg(pers_RXXP=("RXXP16X", "sum"), n_purchases=("RXXP16X", "count"))
    .reset_index()
)
TC1pers["person"] = 1
TC1pers["TC1"] = apply_format(TC1pers["TC1"], TC1name)

# Calculate estimates using survey procedures ---------------------------------

out = svy_total(
    TC1pers,
    ["person", "n_purchases", "pers_RXXP"],
    weight="PERWT16F",
    strata="VARSTR",
    psu="VARPSU",
    domain="TC1",
)

labels = {
    "person": "Number of people with purchase",
    "n_purchases": "Total purchases",
    "pers_RXXP": "Total expenditures",
}
for var, label in labels.items():
    print(f"\n{label}")
    tab = out[out["variable"] == var].rename(columns={"estimate": label})
    print(tab[["TC1", label, "se"]].to_string(index=False))
