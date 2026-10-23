"""Python port of SAS/older_exercises_1996_to_2006/Linking_examples/L5/L5.sas



<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

INSCOV = {1: "Any private", 2: "Public only", 3: "Uninsured"}
VISTYPE = {"ob": "Office-Based", "op": "Outpatient", "er": "Emergency", "pm": "Drug Purchase"}
AGE = {slice(0, 4): "0-4", slice(5, 17): "5-17", slice(18, 24): "18-24", slice(25, 44): "25-44",
       slice(45, 64): "45-64", slice(65, 90): "65-90"}
SEX = {1: "Male", 2: "Female"}
RACETHNX = {1: "Hispanic", 2: "Black, not Hispanic", 3: "Other"}


def wmeans(df, by, vars_, weight, title):
    """PROC MEANS MEAN ... CLASS ... WEIGHT (weighted descriptive means, no SEs)."""
    g = df.groupby(by, observed=True)
    out = pd.DataFrame({"Obs": g.size(), **{v: g.apply(lambda x, v=v: np.average(x[v], weights=x[weight]))
                                            for v in vars_}}).reset_index()
    show(title, out, "{:,.2f}")


# Asthma conditions (ICD9CODX 493) on the 2001 conditions file (HC-061) -----------------
cond = read_meps("h61", ["CONDIDX", "ICD9CODX", "DUPERSID"])
cond = cond.loc[cond.ICD9CODX == "493", ["CONDIDX", "DUPERSID"]]

# MERGE with the condition-event link file (HC-059IF1) BY CONDIDX; IF A & B, then
# PROC SORT NODUPKEY BY EVNTIDX ------------------------------------------------------------
clnk = read_meps("h59if1")
condev = cond.merge(clnk.drop(columns=["DUPERSID"], errors="ignore"), on="CONDIDX", how="inner")
condev = condev.sort_values("EVNTIDX", kind="stable").drop_duplicates("EVNTIDX")

# Prescribed medicines (HC-059A): LINKIDX -> EVNTIDX --------------------------------------
h59a = read_meps("h59a", ["RXRECIDX", "LINKIDX", "RXSF01X", "RXXP01X"]).rename(columns={"LINKIDX": "EVNTIDX"})
pm = condev.merge(h59a, on="EVNTIDX", how="inner")
pm["AMBTOTEV"], pm["AMBFAMEV"] = pm.RXXP01X, pm.RXSF01X

# Aggregate PMED events to person level (FIRST./LAST. RETAIN sums -> groupby().agg()) --
perpmed = pm.groupby("DUPERSID").agg(AMBTOTPD=("AMBTOTEV", "sum"), AMBFAMPD=("AMBFAMEV", "sum")).reset_index()
h60 = read_meps("h60", ["DUPERSID", "INSCOV01", "PERWT01F", "AGE31X", "AGE42X", "AGE53X", "SEX", "RACETHNX"])
pers1 = perpmed.merge(h60, on="DUPERSID", how="left")
pers1["INSCOV01"] = sas_format(pers1.INSCOV01, INSCOV)
wmeans(pers1, "INSCOV01", ["AMBTOTPD", "AMBFAMPD"], "PERWT01F",
       "Average Prescription Expenditures per Person, for Persons with Asthma -- Total and Paid by Family")

# Merge asthma conditions with each event file; SUM() of facility + doctor amounts treats
# missing as 0 ---------------------------------------------------------------------------
ob = condev.merge(read_meps("h59g", ["EVNTIDX", "OBXP01X", "OBSF01X"]), on="EVNTIDX", how="inner")
ob["AMBTOTEV"], ob["AMBFAMEV"] = ob.OBXP01X, ob.OBSF01X
op = condev.merge(read_meps("h59f", ["EVNTIDX", "OPXP01X", "OPFSF01X", "OPDSF01X"]), on="EVNTIDX", how="inner")
op["AMBTOTEV"], op["AMBFAMEV"] = op.OPXP01X, op[["OPFSF01X", "OPDSF01X"]].sum(axis=1, min_count=1)
er = condev.merge(read_meps("h59e", ["EVNTIDX", "ERXP01X", "ERFSF01X", "ERDSF01X"]), on="EVNTIDX", how="inner")
er["AMBTOTEV"], er["AMBFAMEV"] = er.ERXP01X, er[["ERFSF01X", "ERDSF01X"]].sum(axis=1, min_count=1)

# SET ob op er pm BY EVNTIDX (interleave) with IN= flags -> VISTYPE ---------------------
allevnt = pd.concat([ob.assign(VISTYPE="ob"), op.assign(VISTYPE="op"), er.assign(VISTYPE="er"),
                     pm.assign(VISTYPE="pm")]).sort_values("EVNTIDX", kind="stable")
f = allevnt.VISTYPE.map(VISTYPE).value_counts().rename("Frequency").to_frame()
f["Percent"] = 100 * f.Frequency / f.Frequency.sum()
show("VISTYPE", f.reset_index(), "{:,.2f}")

# Person-level totals (sum statements ignore missing), add person characteristics ------
perev = allevnt.groupby("DUPERSID").agg(AMBTOTPD=("AMBTOTEV", "sum"), AMBFAMPD=("AMBFAMEV", "sum")).reset_index()
pers = perev.merge(h60, on="DUPERSID", how="left")
pers["AGE"] = np.select([pers.AGE53X >= 0, pers.AGE42X >= 0, pers.AGE31X >= 0],
                        [pers.AGE53X, pers.AGE42X, pers.AGE31X], np.nan)
pers["AGE"] = pd.Categorical(sas_format(pers.AGE, AGE), categories=list(AGE.values()), ordered=True)
pers["RACETHNX"] = pd.Categorical(sas_format(pers.RACETHNX, RACETHNX), categories=list(RACETHNX.values()),
                                  ordered=True)
pers["SEX"] = pd.Categorical(sas_format(pers.SEX, SEX), categories=list(SEX.values()), ordered=True)
wmeans(pers, ["AGE", "RACETHNX", "SEX"], ["AMBTOTPD", "AMBFAMPD"], "PERWT01F",
       "Average Ambulatory Care Expenditures per Person for Asthma, by Age, Race/Ethnicity and Sex")
