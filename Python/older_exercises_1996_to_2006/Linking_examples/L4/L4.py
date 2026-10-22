# ----------------------------------------------------------------------------
# Program:      c:\meps\prog\Example_L4.sas
#
# Description:  This example shows how to:
#               (1) Identify persons with specific condition(s)
#               (2) Subset condition file to person level
#               (3) Make one variable from five variables
#               (4) Use SUDAAN to calculate standard errors
#
# Input Files:  c:\meps\data\h61.sas7bdat (2001 Conditions)
#               c:\meps\data\h60.sas7bdat (2001 Full-Year Persons)
#
# Python port of SAS/older_exercises_1996_to_2006/Linking_examples/L4/L4.sas
# (the SUDAAN PROC CROSSTAB steps are replaced by samplics via meps_utils.svy_freq)
# ----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, crosstab_list, load_meps, svy_freq  # noqa: E402

pd.set_option("display.width", 200)

OVR = [(1, 4, "1-4")]
OVERALL = {1: "1 Very Serious", 2: "2 Somewhat Serious", 3: "3 Not Very Serious", 4: "4 Not at All", 5: "5 Missing",
           6: "6 Not Have Asthma"}  # fmt: skip
ASTHMA = {1: "1 Has Asthma", 2: "2 No Asthma"}
AGECAT = {1: "1. 0-17", 2: "2. 18+"}
OVRALL = [f"OVRALL{i}" for i in range(1, 6)]

# Identify persons with asthma; keep the first-reported asthma record per person
h61 = load_meps("h61", columns=["DUPERSID", "PANEL01", "CONDRN", "ICD9CODX"] + OVRALL)
h61["ICD9CODX"] = h61["ICD9CODX"].astype(str).str.strip()
first = h61[h61["ICD9CODX"] == "493"].sort_values(["DUPERSID", "CONDRN"], kind="stable").drop_duplicates("DUPERSID")
fmt_ovr = {c: (lambda s: apply_format(s, OVR, missing=".")) for c in OVRALL}
print(crosstab_list(first.assign(**{c: f(first[c]) for c, f in fmt_ovr.items()}), ["PANEL01", "CONDRN"] + OVRALL).to_string(index=False))

# Assign first-reported OVRALLi (> -1) to OVERALL; 5 = missing
asthma = first.copy()
asthma["OVERALL"] = np.nan
for c in reversed(OVRALL):
    asthma["OVERALL"] = np.where(asthma[c] > -1, asthma[c], asthma["OVERALL"])
asthma["OVERALL"] = asthma["OVERALL"].fillna(5)
print(crosstab_list(asthma.assign(OVERALL=apply_format(asthma["OVERALL"], OVERALL)), ["OVERALL"]).to_string(index=False), "\n")

h60 = load_meps("h60", columns=["DUPERSID", "PERWT01F", "VARSTR01", "VARPSU01", "AGE31X", "AGE42X", "AGE53X"])
pers = h60.merge(asthma[["DUPERSID", "OVERALL"]].assign(_a=1), on="DUPERSID", how="left")
pers = pers[pers["PERWT01F"] > 0].copy()
pers["ASTHMA"] = np.where(pers.pop("_a") == 1, 1, 2)  # 1 if P Has Asthma
pers["OVERALL"] = pers["OVERALL"].fillna(6)  # How Asthma Affects Overall Health
age = pers["AGE53X"].where(pers["AGE53X"] >= 0).fillna(pers["AGE42X"].where(pers["AGE42X"] >= 0)).fillna(
    pers["AGE31X"].where(pers["AGE31X"] >= 0)
)
pers["AGECAT"] = np.select([age.between(0, 17.999), age >= 18], [1, 2], default=np.nan)  # Age 0-17, 18+
pers["ASTHMA_F"] = apply_format(pers["ASTHMA"], ASTHMA)
pers["OVERALL_F"] = apply_format(pers["OVERALL"], OVERALL)
pers["AGECAT_F"] = apply_format(pers["AGECAT"], AGECAT, missing=".")
print(pd.crosstab(pers["ASTHMA_F"], pers["AGECAT_F"]), "\n", pd.crosstab(pers["OVERALL_F"], pers["AGECAT_F"]), "\n")

design = dict(weight="PERWT01F", strata="VARSTR01", psu="VARPSU01")
print("ASTHMA")
print(svy_freq(pers, "ASTHMA_F", **design).to_string(index=False), "\n")

# How asthma affects health: SUBPOPN ASTHMA = 1 (domain); TABLES OVERALL OVERALL*AGECAT
print("HOW ASTHMA AFFECTS OVERALL HEALTH (PERSONS WITH ASTHMA)")
res = svy_freq(pers, "OVERALL_F", domain="ASTHMA", **design)
print(res[res["ASTHMA"] == 1].to_string(index=False), "\n")
res = svy_freq(pers, "OVERALL_F", domain=["ASTHMA", "AGECAT_F"], **design)
print(res[res["ASTHMA"] == 1].to_string(index=False))
