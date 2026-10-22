# ----------------------------------------------------------------------------
# Program:      c:\meps\prog\Example_L5.sas
#
# Description:  This example shows how to:
#               (1) Use condition file to identify events for asthma
#               (2) Link conditions to events
#               (3) Merge the condition-event linked file to event files
#               (4) For each event, construct variables with the same name across events
#               (5) Combine facility and doctor expenditures
#               (6) Combine event files, identify type of event
#               (7) Aggregate event-level records to person level
#
# Input Files:  c:\meps\data\h61.sas7bdat (2001 Conditions)
#               c:\meps\data\h59if1.sas7bdat (2001 Condition-Event Link File)
#               c:\meps\data\h59a.sas7bdat (2001 Prescribed Medicines)
#               c:\meps\data\h59e.sas7bdat (2001 Emergency Room Visits)
#               c:\meps\data\h59f.sas7bdat (2001 Outpatient Visits)
#               c:\meps\data\h59g.sas7bdat (2001 Office-Based Medical Provider Visits)
#               c:\meps\data\h60.sas7bdat (2001 Full-Year Persons)
#
# Python port of SAS/older_exercises_1996_to_2006/Linking_examples/L5/L5.sas
# ----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, crosstab_list, load_meps  # noqa: E402

pd.set_option("display.width", 200)

INSCOV = {1: "Any private", 2: "Public only", 3: "Uninsured"}
VISTYPE = {"ob": "Office-Based", "op": "Outpatient", "er": "Emergency", "pm": "Drug Purchase"}
AGE = [(0, 4, "0-4"), (5, 17, "5-17"), (18, 24, "18-24"), (25, 44, "25-44"), (45, 64, "45-64"), (65, 90, "65-90")]
SEX = {1: "Male", 2: "Female"}
RACETHNX = {1: "Hispanic", 2: "Black, not Hispanic", 3: "Other"}


def wmeans(df, by, cols=("AMBTOTPD", "AMBFAMPD")):
    """PROC MEANS MEAN; CLASS by; WEIGHT PERWT01F (weighted means, no survey SEs)."""
    return df.groupby(by).apply(lambda g: pd.Series({c: np.average(g[c], weights=g["PERWT01F"]) for c in cols})).round(2)


# Identify asthma conditions; merge link file (CLNK) & CONDS by CONDIDX
cond = load_meps("h61", columns=["CONDIDX", "ICD9CODX", "DUPERSID"])
cond = cond[cond["ICD9CODX"].astype(str).str.strip() == "493"].drop(columns="ICD9CODX")
clnk = load_meps("h59if1")
condev = cond.merge(clnk.drop(columns=["DUPERSID"], errors="ignore"), on="CONDIDX", how="inner")
condev = condev.sort_values("EVNTIDX", kind="stable").drop_duplicates("EVNTIDX")  # NODUPKEY BY EVNTIDX

# Prescribed medicines
h59a = load_meps("h59a", columns=["RXRECIDX", "LINKIDX", "RXSF01X", "RXXP01X"]).rename(columns={"LINKIDX": "EVNTIDX"})
pm = condev.merge(h59a, on="EVNTIDX", how="inner").assign(AMBTOTEV=lambda d: d["RXXP01X"], AMBFAMEV=lambda d: d["RXSF01X"])

# Aggregate PMED events to person level (Total Paid / Amount Paid by Family: Prescriptions)
perpmed = pm.groupby("DUPERSID").agg(AMBTOTPD=("AMBTOTEV", "sum"), AMBFAMPD=("AMBFAMEV", "sum")).reset_index()
h60 = load_meps("h60", columns=["DUPERSID", "INSCOV01", "PERWT01F", "AGE31X", "AGE42X", "AGE53X", "SEX", "RACETHNX"])
pers1 = perpmed.merge(h60, on="DUPERSID", how="left")
print("Average Prescription Expenditures per Person, for Persons with Asthma -- Total and Paid by Family")
print(wmeans(pers1.assign(INSCOV01=apply_format(pers1["INSCOV01"], INSCOV)), "INSCOV01"), "\n")

# Merge asthma conditions with each event file; same expenditure names across events
h59g = load_meps("h59g", columns=["EVNTIDX", "OBXP01X", "OBSF01X"])
ob = condev.merge(h59g, on="EVNTIDX").assign(AMBTOTEV=lambda d: d["OBXP01X"], AMBFAMEV=lambda d: d["OBSF01X"])
h59f = load_meps("h59f", columns=["EVNTIDX", "OPXP01X", "OPFSF01X", "OPDSF01X"])
op = condev.merge(h59f, on="EVNTIDX").assign(  # combine facility & doctor amounts
    AMBTOTEV=lambda d: d["OPXP01X"], AMBFAMEV=lambda d: d[["OPFSF01X", "OPDSF01X"]].sum(axis=1, min_count=1)
)
h59e = load_meps("h59e", columns=["EVNTIDX", "ERXP01X", "ERFSF01X", "ERDSF01X"])
er = condev.merge(h59e, on="EVNTIDX").assign(
    AMBTOTEV=lambda d: d["ERXP01X"], AMBFAMEV=lambda d: d[["ERFSF01X", "ERDSF01X"]].sum(axis=1, min_count=1)
)

# Combine 4 event files and identify the type of visit or purchase
allevnt = pd.concat([ob.assign(VISTYPE="ob"), op.assign(VISTYPE="op"), er.assign(VISTYPE="er"), pm.assign(VISTYPE="pm")])
allevnt = allevnt.sort_values("EVNTIDX", kind="stable")
print(crosstab_list(allevnt.assign(VISTYPE=allevnt["VISTYPE"].map(VISTYPE)), ["VISTYPE"]).to_string(index=False), "\n")

# Aggregate events to person level (Total / Family Paid, Ambulatory Care for Asthma)
perev = allevnt.groupby("DUPERSID").agg(AMBTOTPD=("AMBTOTEV", "sum"), AMBFAMPD=("AMBFAMEV", "sum")).reset_index()
pers = perev.merge(h60, on="DUPERSID", how="left")
pers["AGE"] = pers["AGE53X"].where(pers["AGE53X"] >= 0).fillna(pers["AGE42X"].where(pers["AGE42X"] >= 0)).fillna(
    pers["AGE31X"].where(pers["AGE31X"] >= 0)
)
pers["AGE"] = apply_format(pers["AGE"], AGE)
pers["RACETHNX"] = apply_format(pers["RACETHNX"], RACETHNX)
pers["SEX"] = apply_format(pers["SEX"], SEX)
print("Ambulatory care expenditures for asthma, by age, race/ethnicity and sex")
for by in ([], ["AGE"], ["RACETHNX"], ["SEX"], ["AGE", "RACETHNX", "SEX"]):
    print(wmeans(pers.assign(ALL="ALL"), by or ["ALL"]), "\n")
