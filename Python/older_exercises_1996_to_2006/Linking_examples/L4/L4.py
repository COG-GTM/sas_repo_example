"""Python port of SAS/older_exercises_1996_to_2006/Linking_examples/L4/L4.sas



<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

OVERALL = {1: "1 Very Serious", 2: "2 Somewhat Serious", 3: "3 Not Very Serious", 4: "4 Not at All", 5: "5 Missing",
           6: "6 Not Have Asthma"}
OVR = {slice(1, 4): "1-4"}
ASTHMA = {1: "1 Has Asthma", 2: "2 No Asthma"}
AGECAT = {1: "1. 0-17", 2: "2. 18+"}
OV = [f"OVRALL{i}" for i in range(1, 6)]

# Persons with asthma (ICD9CODX 493) on the 2001 conditions file (HC-061); keep the
# first-reported asthma record per person ---------------------------------------------
cond = read_meps("h61", ["DUPERSID", "PANEL01", "CONDRN", "ICD9CODX"] + OV)
cond = cond[cond.ICD9CODX == "493"].sort_values(["DUPERSID", "CONDRN"], kind="stable")
first = cond.groupby("DUPERSID").head(1).copy()
print(freq_list(first.assign(**{v: sas_format(first[v], OVR) for v in OV}), ["PANEL01", "CONDRN"] + OV)
      .to_string(index=False), "\n")

# OVERALL = first OVRALLi > -1 (5 = missing)
first["OVERALL"] = np.select([first[v] > -1 for v in OV], [first[v] for v in OV], 5)
show("OVERALL", sas_format(first.OVERALL, OVERALL).value_counts().sort_index().rename("Frequency").reset_index())
print(freq_list(first.assign(OVERALL=sas_format(first.OVERALL, OVERALL), **{v: sas_format(first[v], OVR) for v in OV}),
                ["OVERALL"] + OV).to_string(index=False), "\n")

# MERGE with the 2001 full-year file (HC-060); positive-weight persons ------------------
h60 = read_meps("h60", ["DUPERSID", "PERWT01F", "VARSTR01", "VARPSU01", "AGE31X", "AGE42X", "AGE53X"])
pers = h60.merge(first[["DUPERSID", "OVERALL"]], on="DUPERSID", how="left", indicator=True)
pers = pers[pers.PERWT01F > 0].copy()
pers["ASTHMA"] = np.where(pers._merge == "both", 1, 2)
pers["OVERALL"] = pers.OVERALL.fillna(6)
age = pd.Series(np.select([pers.AGE53X >= 0, pers.AGE42X >= 0, pers.AGE31X >= 0],
                          [pers.AGE53X, pers.AGE42X, pers.AGE31X], np.nan), index=pers.index)
pers["AGECAT"] = np.select([age.between(0, 17.999), age >= 18], [1, 2], np.nan)
pers["ASTHMA_F"] = sas_format(pers.ASTHMA, ASTHMA)
pers["OVERALL_F"] = sas_format(pers.OVERALL, OVERALL)
pers["AGECAT_F"] = sas_format(pers.AGECAT, AGECAT)
print(freq_list(pers, ["ASTHMA_F", "AGECAT_F"]).to_string(index=False), "\n")
print(freq_list(pers, ["OVERALL_F", "AGECAT_F"]).to_string(index=False), "\n")

# SUDAAN PROC CROSSTAB (DESIGN=WR) -------------------------------------------------------
design = dict(weight="PERWT01F", strata="VARSTR01", cluster="VARPSU01")
COLS = ["Frequency", "WgtFreq", "StdDev", "RowPercent", "RowStdErr"]
show("Asthma (all persons)", surveyfreq(pers, ["ASTHMA_F"], **design)[["ASTHMA_F"] + COLS], "{:,.2f}")
# SUBPOPN ASTHMA = 1 -> domain; OVERALL and OVERALL by AGECAT (column percents)
pers["SUB"] = np.where(pers.ASTHMA == 1, 1, 0)
t = surveyfreq(pers, ["SUB", "OVERALL_F"], **design)
show("Persons with asthma: how asthma affects overall health", t[t.SUB == 1][["OVERALL_F"] + COLS], "{:,.2f}")
t = surveyfreq(pers, ["SUB", "AGECAT_F", "OVERALL_F"], **design)
show("Persons with asthma: overall health by age (column percents within age)",
     t[t.SUB == 1][["AGECAT_F", "OVERALL_F"] + COLS], "{:,.2f}")
