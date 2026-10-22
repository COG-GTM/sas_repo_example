# ----------------------------------------------------------------
# PROGRAM:      C:\MEPS\PROG\EXAMPLE07.SAS
#
# DESCRIPTION:  THIS EXAMPLE SHOWS HOW TO COMPUTE PERSON-LEVEL
#               PRESCRIBED MEDICINE EXPENDITURES FOR PERSONS
#               WITH AT LEAST ONE PMED EVENT.
#
# INPUT FILES:  C:\MEPS\DATA\H77A.SAS7BDAT (2003 PMED EVENTS FILE)
#               C:\MEPS\DATA\H79.SAS7BDAT (2003 FULL-YEAR DATA FILE)
#
# Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M7/M7.sas
# ----------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import add_domain, apply_format, load_meps, svy_mean, svy_total  # noqa: E402

pd.set_option("display.width", 200)
pd.set_option("display.float_format", "{:,.0f}".format)

AGECAT = {1: "UNDER 18", 2: "18 - 64", 3: "65 AND OLDER"}
RACETHN = {1: "HISPANIC", 2: "BLACK", 3: "ASIAN", 4: "OTHER"}
INSCOV = {1: "ANY PRIVATE", 2: "PUBLIC ONLY", 3: "UNINSURED"}

# Persons with at least one PMED event (HC-077A)
PUF77A = load_meps("h77a", columns=["DUPERSID"]).drop_duplicates()

cols = ["DUPERSID", "AGE03X", "AGE53X", "AGE42X", "AGE31X", "RXEXP03", "RXMCD03", "RXMCR03", "RXPRV03", "RXSLF03",
        "RACETHNX", "PERWT03F", "VARSTR", "VARPSU", "INSCOV03"]  # fmt: skip
P = load_meps("h79", columns=cols)
P["AGE"] = np.select([P["AGE03X"] >= 0, P["AGE53X"] >= 0, P["AGE42X"] >= 0, P["AGE31X"] >= 0],
                     [P["AGE03X"], P["AGE53X"], P["AGE42X"], P["AGE31X"]], default=P["AGE03X"])  # fmt: skip
P["AGECAT"] = np.select([P["AGE"].between(0, 17.999), P["AGE"].between(18, 64.999), P["AGE"] >= 65], [1, 2, 3], np.nan)
P["OTHER"] = P["RXEXP03"] - (P["RXMCD03"] + P["RXMCR03"] + P["RXPRV03"] + P["RXSLF03"])
EXP = ["RXEXP03", "RXSLF03", "RXPRV03", "RXMCR03", "RXMCD03", "OTHER"]  # TOTAL, OOP, PRIVATE, MEDICARE, MEDICAID, OTHER
for v in EXP:
    P[f"{v}X"] = P[v] / 1000  # in thousands

# SAS uses the PMED-file merge as a filter (IF A). To keep the full design for
# the standard errors we use a domain flag instead of subsetting.
P["HAS_PMED"] = P["DUPERSID"].isin(PUF77A["DUPERSID"]).astype(int)
P = add_domain(P, P["HAS_PMED"] == 1, "PERWT03F", name="PMED_DOMAIN")
design = dict(weight="PERWT03F", strata="VARSTR", psu="VARPSU")
print("RXEXP03, PERSONS WITH AT LEAST ONE PMED EVENT")
res = svy_mean(P, "RXEXP03", domain="HAS_PMED", **design).merge(
    svy_total(P, "RXEXP03", domain="HAS_PMED", **design), on=["HAS_PMED", "variable", "n"], suffixes=("_mean", "_sum")
)
print(res[res["HAS_PMED"] == 1].to_string(index=False), "\n")

# PROC TABULATE (weighted sums and means) for persons with a PMED event
sub = P[P["HAS_PMED"] == 1].copy()
sub["AGECAT"] = apply_format(sub["AGECAT"], AGECAT, missing=".")
sub["RACETHNX"] = apply_format(sub["RACETHNX"], RACETHN)
sub["INSCOV03"] = apply_format(sub["INSCOV03"], INSCOV)
w = sub["PERWT03F"]


def tabulate(variables, stat):
    rows = []
    for cls in (None, "AGECAT", "RACETHNX", "INSCOV03"):
        g = sub.assign(_c="TOTAL").groupby(cls or "_c")
        wsum = g.apply(lambda d: d[variables].mul(d["PERWT03F"], axis=0).sum())
        rows.append(wsum if stat == "SUM" else wsum.div(g["PERWT03F"].sum(), axis=0))
    return pd.concat(rows)


print("2003 TOTAL PMED EXPENDITURES (IN THOUSANDS)")
print(tabulate([f"{v}X" for v in EXP], "SUM"), "\n")
print("2003 MEAN PMED EXPENDITURES")
print(tabulate(EXP, "MEAN"))
