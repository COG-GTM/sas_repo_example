"""Python port of SAS/summary_tables_examples/pmed_therapeutic_class_2016.sas

Example code to replicate estimates from the MEPS-HC Data Tools summary tables

Prescribed drugs, 2016

Purchases and expenditures by Multum therapeutic class name (TC1)
 - Number of people with purchase
 - Total purchases
 - Total expenditures

Input file: <MEPS_DIR>/h188a.ssp (2016 RX event file)

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# Load RX file (SAS: PROC XCOPY) ------------------------------------------------
h188a = read_meps("h188a")

# Aggregate to person-level, by therapeutic class --------------------------------
# (SAS: PROC SORT + PROC MEANS ... BY ...; OUTPUT sum= n=)
by = ["DUPERSID", "VARSTR", "VARPSU", "PERWT16F", "TC1"]
TC1pers = h188a.groupby(by, dropna=False).agg(pers_RXXP=("RXXP16X", "sum"), n_purchases=("RXXP16X", "count"))
TC1pers = TC1pers.reset_index()
TC1pers["person"] = 1

# PROC FORMAT
TC1name = {-9: "Not ascertained", -1: "Inapplicable", 1: "Anti-infectives", 19: "Antihyperlipidemic agents",
           20: "Antineoplastics", 28: "Biologicals", 40: "Cardiovascular agents",
           57: "Central nervous system agents", 81: "Coagulation modifiers", 87: "Gastrointestinal agents",
           97: "Hormones/hormone modifiers", 105: "Miscellaneous agents", 113: "Genitourinary tract agents",
           115: "Nutritional products", 122: "Respiratory agents", 133: "Topical agents",
           218: "Alternative medicines", 242: "Psychotherapeutic agents", 254: "Immunologic agents",
           358: "Metabolic agents"}

# Calculate estimates using survey procedures (DOMAIN TC1) -----------------------
out = surveymeans(TC1pers, ["person", "n_purchases", "pers_RXXP"], weight="PERWT16F", strata="VARSTR",
                  cluster="VARPSU", domain="TC1")
out["TC1"] = sas_format(out.TC1, TC1name)

for v, label in [("person", "Number of people with purchase"), ("n_purchases", "Total purchases"),
                 ("pers_RXXP", "Total expenditures")]:
    show(label, out[out.VarName == v][["TC1", "Sum", "StdDev"]].rename(columns={"Sum": label}),
         floatfmt="{:,.2f}")
