"""Python port of SAS/summary_tables_examples/pmed_prescribed_drug_2016.sas

Example code to replicate estimates from the MEPS-HC Data Tools summary tables

Prescribed drugs, 2016

Purchases and expenditures by generic drug name (RXDRGNAM)
 - Number of people with purchase
 - Total purchases
 - Total expenditures

Input file: <MEPS_DIR>/h188a.ssp (2016 RX event file)

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path


sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# Load RX file (SAS: PROC XCOPY) ------------------------------------------------
h188a = read_meps("h188a")
RX = h188a.copy()

# Aggregate to person-level, by drug name ---------------------------------------
# (SAS: PROC SORT + PROC MEANS ... BY ...; OUTPUT sum= n=)
by = ["DUPERSID", "VARSTR", "VARPSU", "PERWT16F", "RXDRGNAM"]
RX_pers = RX.groupby(by, dropna=False).agg(pers_RXXP=("RXXP16X", "sum"), n_purchases=("RXXP16X", "count"))
RX_pers = RX_pers.reset_index()
RX_pers["person"] = 1

# Calculate estimates using survey procedures (DOMAIN RXDRGNAM) -----------------
out = surveymeans(RX_pers, ["person", "n_purchases", "pers_RXXP"], weight="PERWT16F", strata="VARSTR",
                  cluster="VARPSU", domain="RXDRGNAM")

for v, label in [("person", "Number of people with purchase"), ("n_purchases", "Total purchases"),
                 ("pers_RXXP", "Total expenditures")]:
    show(label, out[out.VarName == v][["RXDRGNAM", "Sum", "StdDev"]].rename(columns={"Sum": label}),
         floatfmt="{:,.2f}")
