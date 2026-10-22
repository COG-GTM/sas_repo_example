# ----------------------------------------------------------------------------
# Program:      c:\meps\prog\Example_L3.sas
#
# Description:  This example shows how to:
#               (1) Aggregate event records to the person level
#               (2) Make 1 annual variable from 3 round variables
#               (3) Make a categorical variable from a continuous variable
#               (4) Use SUDAAN to calculate standard errors
#
# Input Files:  c:\meps\data\h59g.sas7bdat (2001 Office-Based Visits)
#               c:\meps\data\h60.sas7bdat (2001 Full-Year File)
#
# Python port of SAS/older_exercises_1996_to_2006/Linking_examples/L3/L3.sas
# (the SUDAAN PROC CROSSTAB step is replaced by samplics via meps_utils.svy_freq)
# ----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, crosstab_list, load_meps, svy_freq  # noqa: E402

pd.set_option("display.width", 200)

INSCOV = {1: "1 Any Private", 2: "2 Public Only", 3: "3 Uninsured"}
INSURED = {1: "1 Insured", 2: "2 Uninsured"}
AGECAT = {1: "1. 0-3", 2: "2. 4-7", 3: "3. 8-11", 4: "4. 12-15", 5: "5. 16-17", 6: "6. 18+"}
GENCKUP = {1: "1 General Checkup", 2: "2 No General Checkup"}

# 2001 office-based medical provider visits: persons with a visit for a general
# check-up (VSTCTGRY = 1) and their expenditures
#  GENCKUP = Had Office-Based General Checkup; AMBTOTPD = Total Amount Paid;
#  AMBFAMPD = Amount Paid by Family
ob = load_meps("h59g", columns=["DUPERSID", "VSTCTGRY", "OBXP01X", "OBSF01X"])
ck = ob[ob["VSTCTGRY"] == 1]
h59g = ck.groupby("DUPERSID").agg(AMBTOTPD=("OBXP01X", "sum"), AMBFAMPD=("OBSF01X", "sum")).assign(GENCKUP=1)
h59g = h59g.reindex(ob["DUPERSID"].unique()).reset_index().rename(columns={"index": "DUPERSID"})
h59g[["AMBTOTPD", "AMBFAMPD"]] = h59g[["AMBTOTPD", "AMBFAMPD"]].fillna(0)
print(crosstab_list(h59g, ["GENCKUP"]).to_string(index=False), "\n")

h60 = load_meps("h60", columns=["DUPERSID", "PERWT01F", "VARSTR01", "VARPSU01", "AGE31X", "AGE42X", "AGE53X", "INSCOV01"])
h60 = h60[h60["PERWT01F"] > 0].copy()  # subset to positive weight persons
# AGE = last nonmissing age in 2001
h60["AGE"] = np.select([h60["AGE53X"] >= 0, h60["AGE42X"] >= 0, h60["AGE31X"] >= 0],
                       [h60["AGE53X"], h60["AGE42X"], h60["AGE31X"]], default=np.nan)  # fmt: skip
a = h60["AGE"]
h60["AGECAT"] = (a >= 0).astype(int) + (a > 3) + (a > 7) + (a > 11) + (a > 15) + (a > 17)
h60["INSURED"] = np.where(h60["INSCOV01"] > 2, 2, 1)  # Had Health Insurance in 2001

print(crosstab_list(h60.assign(INSURED=apply_format(h60["INSURED"], INSURED),
                               INSCOV01=apply_format(h60["INSCOV01"], INSCOV)), ["INSURED", "INSCOV01"]).to_string(index=False))  # fmt: skip
print(crosstab_list(h60.assign(AGECAT=apply_format(h60["AGECAT"], AGECAT)), ["AGECAT"]).to_string(index=False))
print(h60.groupby("AGECAT")["AGE"].agg([lambda s: s.isna().sum(), "min", "max"]).rename(index=AGECAT), "\n")

pers = h60.drop(columns=["AGE31X", "AGE42X", "AGE53X", "INSCOV01"]).merge(h59g, on="DUPERSID", how="left")
pers["GENCKUP"] = pers["GENCKUP"].fillna(2)
print(crosstab_list(pers.assign(GENCKUP=apply_format(pers["GENCKUP"], GENCKUP)), ["GENCKUP"]).to_string(index=False))

# SUBPOPN AGECAT = 6 (adults) as a domain; TABLES GENCKUP GENCKUP*INSURED (column %)
pers["ADULT"] = np.where(pers["AGECAT"] == 6, 1, 0)
pers["GENCKUP_F"] = apply_format(pers["GENCKUP"], GENCKUP)
pers["INSURED_F"] = apply_format(pers["INSURED"], INSURED)
design = dict(weight="PERWT01F", strata="VARSTR01", psu="VARPSU01")
print("\nGENERAL CHECKUP, ADULTS 18+")
res = svy_freq(pers, "GENCKUP_F", domain="ADULT", **design)
print(res[res["ADULT"] == 1].to_string(index=False))
print("\nGENERAL CHECKUP BY INSURANCE STATUS (COLUMN %), ADULTS 18+")
res = svy_freq(pers, "GENCKUP_F", domain=["ADULT", "INSURED_F"], **design)
print(res[res["ADULT"] == 1].to_string(index=False), "\n")

# Weighted means of amounts paid, adults with a general checkup, by insurance
sub = pers[(pers["AGECAT"] == 6) & (pers["GENCKUP"] == 1)]
print(sub.groupby("INSURED_F").apply(lambda g: pd.Series({
    "N": len(g),
    "AMBTOTPD": np.average(g["AMBTOTPD"], weights=g["PERWT01F"]),
    "AMBFAMPD": np.average(g["AMBFAMPD"], weights=g["PERWT01F"]),
})).round(2))  # fmt: skip
