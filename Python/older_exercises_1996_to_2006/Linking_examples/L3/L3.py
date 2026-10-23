"""Python port of SAS/older_exercises_1996_to_2006/Linking_examples/L3/L3.sas



<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# 2001 office-based visits (HC-059G): persons with a general check-up and its expenses --
h59g = read_meps("h59g", ["DUPERSID", "VSTCTGRY", "OBXP01X", "OBSF01X"])
ck = h59g[h59g.VSTCTGRY == 1]
h59g = pd.DataFrame({"GENCKUP": ck.groupby("DUPERSID").size().clip(upper=1).astype(float),
                     "AMBTOTPD": ck.groupby("DUPERSID").OBXP01X.sum(),
                     "AMBFAMPD": ck.groupby("DUPERSID").OBSF01X.sum()}).reset_index()
print(freq_list(h59g, ["GENCKUP"]).to_string(index=False), "\n")

# 2001 full-year file (HC-060), positive-weight persons -----------------------------------
h60 = read_meps("h60", ["DUPERSID", "PERWT01F", "VARSTR01", "VARPSU01", "AGE31X", "AGE42X", "AGE53X", "INSCOV01"])
h60 = h60[h60.PERWT01F > 0].copy()
h60["AGE"] = np.select([h60.AGE53X >= 0, h60.AGE42X >= 0, h60.AGE31X >= 0], [h60.AGE53X, h60.AGE42X, h60.AGE31X],
                       np.nan)
a = h60.AGE
h60["AGECAT"] = (a >= 0).astype(int) + (a > 3) + (a > 7) + (a > 11) + (a > 15) + (a > 17)
h60["INSURED"] = np.where(h60.INSCOV01 > 2, 2, 1)
INSURED = {1: "1 Insured", 2: "2 Uninsured"}
GENCKUP = {1: "1 General Checkup", 2: "2 No General Checkup"}
t = h60.assign(INSURED=sas_format(h60.INSURED, INSURED),
               INSCOV01=sas_format(h60.INSCOV01, {1: "1 Any Private", 2: "2 Public Only", 3: "3 Uninsured"}))
print(freq_list(t, ["INSURED", "INSCOV01"]).to_string(index=False), "\n")
print(freq_list(h60, ["AGECAT"]).to_string(index=False), "\n")
print(h60.groupby("AGECAT").AGE.agg(["count", "min", "max"]), "\n")

# MERGE h59g h60 (IN=A) BY DUPERSID; IF A -> left join on the person file -------------
pers = h60.drop(columns=["AGE31X", "AGE42X", "AGE53X", "INSCOV01"]).merge(h59g, on="DUPERSID", how="left")
pers["GENCKUP"] = pers.GENCKUP.fillna(2)
print(freq_list(pers.assign(GENCKUP=sas_format(pers.GENCKUP, GENCKUP)), ["GENCKUP"]).to_string(index=False), "\n")

# SUDAAN PROC CROSSTAB, SUBPOPN AGECAT = 6 -> subpopulation as a domain -----------------
pers["ADULT"] = np.where(pers.AGECAT == 6, 1, 0)
pers["GENCKUP_F"] = sas_format(pers.GENCKUP, GENCKUP)
pers["INSURED_F"] = sas_format(pers.INSURED, INSURED)
design = dict(weight="PERWT01F", strata="VARSTR01", cluster="VARPSU01")
COLS = ["Frequency", "WgtFreq", "StdDev", "RowPercent", "RowStdErr"]
t1 = surveyfreq(pers, ["ADULT", "GENCKUP_F"], **design)
show("Persons 18+: general checkup (Col Percent)", t1[t1.ADULT == 1][["GENCKUP_F"] + COLS], "{:,.2f}")
t2 = surveyfreq(pers, ["ADULT", "INSURED_F", "GENCKUP_F"], **design)
show("Persons 18+: general checkup by insurance status (Col Percent within insurance status)",
     t2[t2.ADULT == 1][["INSURED_F", "GENCKUP_F"] + COLS], "{:,.2f}")

# PROC MEANS ... WEIGHT PERWT01F (weighted descriptive means for adults with a checkup) -
sub = pers[(pers.AGECAT == 6) & (pers.GENCKUP == 1)]
g = sub.groupby("INSURED_F")
show("Adults with a general checkup: weighted mean amount paid",
     pd.DataFrame({"N": g.size(), **{v: g.apply(lambda x, v=v: np.average(x[v], weights=x.PERWT01F))
                                       for v in ["AMBTOTPD", "AMBFAMPD"]}}).reset_index(), "{:,.2f}")
