# ----------------------------------------------------------------------
# PROGRAM:     C:\MEPS\PROG\M8.SAS
#
# DESCRIPTION: THIS EXAMPLE SHOWS HOW TO COMPUTE PRESCRIBED
#              MEDICINE EXPENDITURES ASSOCIATED WITH CANCER
#              CONDITIONS.
#              IN ADDITION TO THE MEDICAL CONDITIONS AND FULL-
#              YEAR FILES, THE PRESCRIBED MEDICINES EVENT FILE AND
#              THE CLNK FILE ARE USED.
#              THE PMED EXPENDITURES ARE SUMMED TO THE EVENT
#              LEVEL.
#              MODIFIED SOURCE OF PAYMENT (SOP) CATEGORIES
#              ARE CREATED AS COLUMN CATEGORIES.
#              ROW CATEGORIES ARE AGE AND RACE/ETHNICITY (FROM
#              THE FULL-YEAR FILE).
#              USE PROC TABULATE TO COMPUTE AND DISPLAY OUTPUT.
#
# INPUT FILES: (1) C:\MEPS\DATA\H97.SAS7BDAT (2005 FULL-YEAR DATA FILE)
#              (2) C:\MEPS\DATA\H96.SAS7BDAT (2005 MEDICAL CONDITIONS FILE)
#              (3) C:\MEPS\DATA\H94I1.SAS7BDAT (2005 CLNK FILE)
#                    THIS FILE LINKS CONDITIONS TO EVENTS
#              (4) C:\MEPS\DATA\H94A.SAS7BDAT (2005 PRESCRIBED MED. FILE)
#
# Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M8/M8.sas
# ----------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, load_meps  # noqa: E402

pd.set_option("display.width", 200)

AGECAT = {1: " < 18", 2: "18-64", 3: "65+"}
RACETHNB = {1: "HISPANIC", 2: "BLACK", 3: "ASIAN", 4: "OTHER"}
EXAMPLE_IDS = ["32663024", "33634027"]

# People with cancer (CCCODEX 011-045) from the 2005 condition file
cond = load_meps("h96", columns=["DUPERSID", "CCCODEX", "CONDIDX"])
cond05 = cond[cond["CCCODEX"].between("011", "045")][["DUPERSID", "CONDIDX"]]

# Prescribed medicines expenses from the 2005 PMED file
pm = load_meps("h94a", columns=["DUPERSID", "RXRECIDX", "LINKIDX", "RXSF05X", "RXMD05X", "RXPV05X", "RXMR05X", "RXXP05X",
                                "RXNDC"]).sort_values(["LINKIDX", "RXRECIDX"])  # fmt: skip
print(pm[pm["DUPERSID"].isin(EXAMPLE_IDS)].rename(columns={"RXXP05X": "TOTAL EXP", "RXSF05X": "OOP", "RXMD05X": "MEDICAID",
      "RXMR05X": "MEDICARE", "RXPV05X": "PRIVATE"}).to_string(index=False), "\n")  # fmt: skip

# Sum PMED expenses to the event level (last RXNDC per LINKIDX, as in SAS)
pmedevnt = pm.groupby("LINKIDX").agg(
    DUPERSID=("DUPERSID", "last"), RXNDC=("RXNDC", "last"), OOP=("RXSF05X", "sum"), MEDICARE=("RXMR05X", "sum"),
    MEDICAID=("RXMD05X", "sum"), PRIVATE=("RXPV05X", "sum"), TOTRX05=("RXXP05X", "sum"),
).reset_index().rename(columns={"LINKIDX": "EVNTIDX"})  # fmt: skip
oth = pmedevnt["TOTRX05"] - pmedevnt[["MEDICARE", "MEDICAID", "PRIVATE", "OOP"]].sum(axis=1)
pmedevnt["OTHER"] = np.sign(oth) * np.floor(oth.abs() + 0.5)  # SAS ROUND()
show = ["DUPERSID", "EVNTIDX", "TOTRX05", "OOP", "PRIVATE", "MEDICARE", "MEDICAID", "OTHER"]
print(pmedevnt.loc[pmedevnt["DUPERSID"].isin(EXAMPLE_IDS), show[:2] + ["RXNDC"] + show[2:]].to_string(index=False), "\n")

# Merge conditions to PMED events through the CLNK file
clnk = load_meps("h94if1", columns=["CONDIDX", "EVNTIDX"])
condclnk = cond05.merge(clnk, on="CONDIDX", how="left")
condpmed = condclnk.merge(pmedevnt.drop(columns="DUPERSID"), on="EVNTIDX", how="inner").sort_values(["EVNTIDX", "CONDIDX"])
show = ["DUPERSID", "CONDIDX", "EVNTIDX", "TOTRX05", "OOP", "PRIVATE", "MEDICARE", "MEDICAID", "OTHER"]
print(condpmed.loc[condpmed["DUPERSID"].isin(EXAMPLE_IDS), show].to_string(index=False), "\n")

# De-duplicate expenditures (one record per event)
condpmed = condpmed.drop_duplicates("EVNTIDX")
print(condpmed.loc[condpmed["DUPERSID"].isin(EXAMPLE_IDS), show].to_string(index=False), "\n")

# Age (latest valid) and race/ethnicity from the full-year file
fy = load_meps("h97", columns=["DUPERSID", "AGE05X", "AGE53X", "AGE42X", "AGE31X", "RACETHNX", "PERWT05F"])
age = np.select([fy["AGE05X"] >= 0, fy["AGE53X"] >= 0, fy["AGE42X"] >= 0, fy["AGE31X"] >= 0],
                [fy["AGE05X"], fy["AGE53X"], fy["AGE42X"], fy["AGE31X"]], default=fy["AGE05X"])  # fmt: skip
fy["AGECAT"] = np.select([(age >= 0) & (age < 18), (age >= 18) & (age < 65), age >= 65], [1, 2, 3], np.nan)
temp = condpmed.merge(fy[["DUPERSID", "AGECAT", "RACETHNX", "PERWT05F"]], on="DUPERSID", how="left")
EXP = ["TOTRX05", "OOP", "PRIVATE", "MEDICARE", "MEDICAID", "OTHER"]
temp[EXP] = temp[EXP] / 1000
temp["AGECAT"] = apply_format(temp["AGECAT"], AGECAT, missing=".")
temp["RACETHNX"] = apply_format(temp["RACETHNX"], RACETHNB)

# PROC TABULATE: weighted sums (in thousands) by ALL, AGECAT, RACETHNX
wexp = temp[EXP].mul(temp["PERWT05F"], axis=0)
out = pd.concat([wexp.sum().to_frame("TOTAL").T, wexp.groupby(temp["AGECAT"]).sum(), wexp.groupby(temp["RACETHNX"]).sum()])
print(out.round(0).to_string(float_format="{:,.0f}".format))
