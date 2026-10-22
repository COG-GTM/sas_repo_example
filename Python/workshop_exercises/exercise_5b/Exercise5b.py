# -----------------------------------------------------------------------------
# DESCRIPTION:  THIS PROGRAM ILLUSTRATES HOW TO CONSTRUCT INSURANCE STATUS VARIABLES FROM
#               MONTHLY INSURANCE VARIABLES (see below) IN THE PERSON-LEVEL DATA
# TRImm15X   Covered by TRICARE/CHAMPVA in mm (Ed)
# MCRmm15    Covered by Medicare in mm
# MCRmm15X   Covered by Medicare in mm (Ed)
# MCDmm15    Covered by Medicaid or SCHIP in mm
# MCDmm15X   Covered by Medicaid or SCHIP in mm  (Ed)
# OPAmm15    Covered by Other Public A Ins in mm
# OPBmm15    Covered by Other Public B Ins in mm
# PUBmm15X   Covered by Any Public Ins in mm (Ed)
# PEGmm15    Covered by Empl Union Ins in mm
# PDKmm15    Coverer by Priv Ins (Source Unknown) in mm
# PNGmm15    Covered by Nongroup Ins in mm
# POGmm15    Covered by Other Group Ins in mm
# PRSmm15    Covered by Self-Emp Ins in mm
# POUmm15    Covered by Holder Outside of RU in mm
# PRImm15    Covered by Private Ins in mm
# where mm = JA-DE  (January - December)
# INPUT FILE:   C:\MEPS\SAS\DATA\H181.SAS7BDAT (2015 FY PUF DATA)
#
# Python port of SAS/workshop_exercises/exercise_5b/Exercise5b.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, crosstab_list, load_meps, svy_mean, svy_total  # noqa: E402

pd.set_option("display.width", 200)

YR = "15"
print("2018 AHRQ MEPS DATA USERS WORKSHOP")
print(f"EXERCISE7: HOW TO CONSTRUCT INSURANCE STATUS VARIABLES, USING FY {YR} DATA\n")

RACETHX = {1: "1 HISPANIC", 2: "2 WHITE", 3: "3 BLACK", 4: "4 ASIAN", 5: "5 OTHER RACE"}
MONTHS = ["JA", "FE", "MA", "AP", "MY", "JU", "JL", "AU", "SE", "OC", "NO", "DE"]


def arr(prefix, suffix=""):
    """SAS ARRAY of the 12 monthly variables, e.g. PEGJA15 ... PEGDE15."""
    return [f"{prefix}{m}{YR}{suffix}" for m in MONTHS]


ARRAYS = {k: arr(k) for k in ("PEG", "POU", "PDK", "PNG", "POG", "PRS", "PRX", "PRI", "OPA", "OPB")}
ARRAYS.update({k: arr(k, "X") for k in ("INS", "MCD", "MCR", "TRI")})

# 1) COUNT # OF MONTHS WITH INSURANCE -----------------------------------------

keep = ["DUPERSID", "RACETHX", "VARSTR", "VARPSU", f"PERWT{YR}F"] + sum(ARRAYS.values(), [])
FY1 = load_meps("h181", columns=keep)
A = {k: FY1[v].to_numpy() for k, v in ARRAYS.items()}

FY1["PRI_N"] = (A["PRI"] == 1).sum(axis=1)  # # OF MONTHS COV BY PRIVATE INSU
FY1["INS_N"] = (A["INS"] == 1).sum(axis=1)  # # OF MONTHS COV BY ANY INSU
FY1["UNINS_N"] = (A["INS"] == 2).sum(axis=1)  # # OF MONTHS WITHOUT INSU
FY1["MCD_N"] = (A["MCD"] == 1).sum(axis=1)  # # OF MONTHS COV BY MEDICAID
FY1["MCR_N"] = (A["MCR"] == 1).sum(axis=1)  # # OF MONTHS COV BY MEDICARE
FY1["TRI_N"] = (A["TRI"] == 1).sum(axis=1)  # # OF MONTHS COV BY TRICARE
FY1["OPAB_N"] = ((A["OPA"] == 1) | (A["OPB"] == 1)).sum(axis=1)  # OTHER PUBLIC A OR B
FY1["GRP_N"] = ((A["PEG"] == 1) | (A["TRI"] == 1) | (A["POU"] == 1) | (A["PDK"] == 1)).sum(axis=1)  # PRIVATE GROUP
FY1["NG_N"] = ((A["PRX"] == 1) | (A["PNG"] == 1) | (A["POG"] == 1) | (A["PRS"] == 1)).sum(axis=1)  # NON-GROUP
FY1["PUB_N"] = ((A["MCR"] == 1) | (A["MCD"] == 1) | (A["OPA"] == 1) | (A["OPB"] == 1)).sum(axis=1)  # PUBLIC
FY1["REF_N"] = (A["INS"] > 0).sum(axis=1)  # # OF MONTHS IN MEPS SURVEY

counts = ["PRI_N", "INS_N", "UNINS_N", "MCD_N", "MCR_N", "TRI_N", "OPAB_N", "GRP_N", "NG_N", "PUB_N", "REF_N"]
print("CREATE COUNT VARIABLES FOR # OF MONTHS WITH INSURANCE")
for c in counts:
    print(crosstab_list(FY1, [c]).T.to_string(header=False))
print("\nSAMPLE DUMP TO CHECK THE COUNT VARIABLES (USING PRI_N AS EXAMPLE)")
print(FY1[["DUPERSID"] + ARRAYS["PRI"] + ["PRI_N"]].head(30).to_string(index=False))

# 2) CREATE FLAGS FOR VARIOUS TYPES OF INSU -----------------------------------

FY2 = FY1.copy()
FY2["FULL_INSU"] = (FY2["UNINS_N"] == 0).astype(int)  # INSURED FOR FULL YEAR
FY2["GROUP_INS1"] = (FY2["GRP_N"] > 0).astype(int)  # EVER INSURED BY PRIVATE GROUP
FY2["GROUP_INS2"] = ((FY2["GRP_N"] > 0) & (FY2["GRP_N"] == FY2["REF_N"])).astype(int)  # PRIVATE GROUP FULL YEAR
FY2["NG_INS"] = (FY2["NG_N"] > 0).astype(int)  # EVER INSURED BY PRIVATE NON-GROUP

print("\nSUPPORTING CROSSTABS TO VERIFY THE CREATION OF THE FLAGS")
for t in (["FULL_INSU"], ["FULL_INSU", "UNINS_N"], ["FULL_INSU", "INS_N", "REF_N"], ["GROUP_INS1"],
          ["GROUP_INS1", "GRP_N"], ["GROUP_INS2"], ["GROUP_INS2", "GRP_N", "REF_N"], ["NG_INS"], ["NG_INS", "NG_N"]):  # fmt: skip
    print(crosstab_list(FY2, t))

# 3) CALCULATE % OF PERSONS COVERED BY INSU (DOMAIN RACETHX) -----------------

print("\n% AND POPULATION WITH INSU")
FY2["RACETHX"] = apply_format(FY2["RACETHX"], RACETHX)
flags = ["FULL_INSU", "GROUP_INS1", "GROUP_INS2", "NG_INS"]
design = dict(weight=f"PERWT{YR}F", strata="VARSTR", psu="VARPSU")
overall = svy_mean(FY2, flags, **design).merge(
    svy_total(FY2, flags, **design), on=["variable", "n"], suffixes=("_mean", "_sum")
)
print(overall.to_string(index=False), "\n")
by_race = svy_mean(FY2, flags, domain="RACETHX", **design).merge(
    svy_total(FY2, flags, domain="RACETHX", **design), on=["RACETHX", "variable", "n"], suffixes=("_mean", "_sum")
)
print(by_race.sort_values(["RACETHX"], kind="stable").to_string(index=False))
