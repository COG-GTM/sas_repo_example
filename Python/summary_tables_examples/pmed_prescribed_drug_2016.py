# -----------------------------------------------------------------------------
# Example code to replicate estimates from the MEPS-HC Data Tools summary tables
#
# Prescribed drugs, 2016
#
# Purchases and expenditures by generic drug name (RXDRGNAM)
#  - Number of people with purchase
#  - Total purchases
#  - Total expenditures
#
# Input file: C:\MEPS\h188a.ssp (2016 RX event file)
#
# Python port of SAS/summary_tables_examples/pmed_prescribed_drug_2016.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import MEPS_DIR, read_meps, svy_total  # noqa: E402

pd.set_option("display.width", 200)
pd.set_option("display.max_rows", 100)

# Load datasets ---------------------------------------------------------------
# For 1996-2013, need to merge RX event file with Multum Lexicon Addendum
#  file to get therapeutic class categories and generic drug names

# Load RX file
h188a = read_meps(f"{MEPS_DIR}/h188a.ssp")

# Aggregate to person-level (PROC SORT + PROC MEANS BY ...) -------------------

RX = h188a.copy()
by = ["DUPERSID", "VARSTR", "VARPSU", "PERWT16F", "RXDRGNAM"]
RX_pers = (
    RX.groupby(by)
    .agg(pers_RXXP=("RXXP16X", "sum"), n_purchases=("RXXP16X", "count"))
    .reset_index()
)
RX_pers["person"] = 1

# Calculate estimates using survey procedures ---------------------------------

out = svy_total(
    RX_pers,
    ["person", "n_purchases", "pers_RXXP"],
    weight="PERWT16F",
    strata="VARSTR",
    psu="VARPSU",
    domain="RXDRGNAM",
)

labels = {
    "person": "Number of people with purchase",
    "n_purchases": "Total purchases",
    "pers_RXXP": "Total expenditures",
}
for var, label in labels.items():
    print(f"\n{label}")
    tab = out[out["variable"] == var].rename(columns={"estimate": label})
    print(tab[["RXDRGNAM", label, "se"]].to_string(index=False))
