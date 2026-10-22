# -----------------------------------------------------------------------------
# DESCRIPTION:  THIS PROGRAM ILLUSTRATES HOW TO CALCULATE EXPENDITURES FOR ALL EVENTS ASSOCIATED WITH A CONDITION
#               THE CONDITION USED IN THIS EXERCISE IS DIABETES (CCS CODE=049 OR 050)
# THE DEFINITION OF 61 CONDITIONS BASED ON CCS CODE
#   1  Infectious diseases                                           : CCS CODE = 1-9
#   2  Cancer                                                        : CCS CODE = 11-45
#   3  Non-malignant neoplasm                                        : CCS CODE = 46, 47
#   4  Thyroid disease                                               : CCS CODE = 48
#   5  Diabetes mellitus                                             : CCS CODE = 49,50
#   6  Other endocrine, nutritional & immune disorder                : CCS CODE = 51, 52, 54 - 58
#   7  Hyperlipidemia                                                : CCS CODE = 53
#   8  Anemia and other deficiencies                                 : CCS CODE = 59
#   9  Hemorrhagic, coagulation, and disorders of White Blood cells  : CCS CODE = 60-64
#   10 Mental disorders                                              : CCS CODE = 650-670
#   11 CNS infection                                                 : CCS CODE = 76-78
#   12 Hereditary, degenerative and other nervous system disorders   : CCS CODE = 79-81
#   13 Paralysis                                                     : CCS CODE = 82
#   14 Headache                                                      : CCS CODE = 84
#   15 Epilepsy and convulsions                                      : CCS CODE = 83
#   16 Coma, brain damage                                            : CCS CODE = 85
#   17 Cataract                                                      : CCS CODE = 86
#   18 Glaucoma                                                      : CCS CODE = 88
#   19 Other eye disorders                                           : CCS CODE = 87, 89-91
#   20 Otitis media                                                  : CCS CODE = 92
#   21 Other CNS disorders                                           : CCS CODE = 93-95
#   22 Hypertension                                                  : CCS CODE = 98,99
#   23 Heart disease                                                 : CCS CODE = 96, 97, 100-108
#   24 Cerebrovascular disease                                       : CCS CODE = 109-113
#   25 Other circulatory conditions arteries, veins, and lymphatics  : CCS CODE = 114 -121
#   26 Pneumonia                                                     : CCS CODE = 122
#   27 Influenza                                                     : CCS CODE = 123
#   28 Tonsillitis                                                   : CCS CODE = 124
#   29 Acute Bronchitis and URI                                      : CCS CODE = 125 , 126
#   30 COPD, asthma                                                  : CCS CODE = 127-134
#   31 Intestinal infection                                          : CCS CODE = 135
#   32 Disorders of teeth and jaws                                   : CCS CODE = 136
#   33 Disorders of mouth and esophagus                              : CCS CODE = 137
#   34 Disorders of the upper GI                                     : CCS CODE = 138-141
#   35 Appendicitis                                                  : CCS CODE = 142
#   36 Hernias                                                       : CCS CODE = 143
#   37 Other stomach and intestinal disorders                        : CCS CODE = 144- 148
#   38 Other GI                                                      : CCS CODE = 153-155
#   39 Gallbladder, pancreatic, and liver disease                    : CCS CODE = 149-152
#   40 Kidney Disease                                                : CCS CODE = 156-158, 160, 161
#   41 Urinary tract infections                                      : CCS CODE = 159
#   42 Other urinary                                                 : CCS CODE = 162,163
#   43 Male genital disorders                                        : CCS CODE = 164-166
#   44 Non-malignant breast disease                                  : CCS CODE = 167
#   45 Female genital disorders, and contraception                   : CCS CODE = 168-176
#   46 Complications of pregnancy and birth                          : CCS CODE = 177-195
#   47 Normal birth/live born                                        : CCS CODE = 196, 218
#   48 Skin disorders                                                : CCS CODE = 197-200
#   49 Osteoarthritis and other non-traumatic joint disorders        : CCS CODE = 201-204
#   50 Back problems                                                 : CCS CODE = 205
#   51 Other bone and musculoskeletal  disease                       : CCS CODE = 206-209, 212
#   52 Systemic lupus and connective tissues disorders               : CCS CODE = 210-211
#   53 Congenital anomalies                                          : CCS CODE = 213-217
#   54 Perinatal Conditions                                          : CCS CODE = 219-224
#   55 Trauma-related disorders                                      : CCS CODE = 225-236, 239, 240, 244
#   56 Complications of surgery or device                            : CCS CODE = 237, 238
#   57 Poisoning by medical and non-medical substances               : CCS CODE = 241 - 243
#   58 Residual Codes                                                : CCS CODE = 259
#   59 Other care and screening                                      : CCS CODE = 10, 254-258
#   60 Symptoms                                                      : CCS CODE = 245-252
#   61 Allergic reactions                                            : CCS CODE = 253
# INPUT FILES:  1) C:\MEPS\SAS\DATA\H181.SAS7BDAT    (2015 FY PUF DATA)
#               2) C:\MEPS\SAS\DATA\H180.SAS7BDAT    (2015 CONDITION PUF DATA)
#               3) C:\MEPS\SAS\DATA\H178A.SAS7BDAT   (2015 PMED PUF DATA)
#               4) C:\MEPS\SAS\DATA\H178D.SAS7BDAT   (2015 INPATIENT VISITS PUF DATA)
#               5) C:\MEPS\SAS\DATA\H178E.SAS7BDAT   (2015 EROM VISITS PUF DATA)
#               6) C:\MEPS\SAS\DATA\H178F.SAS7BDAT   (2015 OUTPATIENT VISITS PUF DATA)
#               7) C:\MEPS\SAS\DATA\H178G.SASBDAT   (2015 OFFICE-BASED VISITS PUF DATA)
#               8) C:\MEPS\SAS\DATA\H178H.SAS7BDAT   (2015 HOME HEALTH PUF DATA)
#               9) C:\MEPS\SAS\DATA\H178IF1.SAS7BDAT  (2015 CONDITION-EVENT LINK PUF DATA)
#
# Python port of SAS/workshop_exercises/exercise_3b/Exercise3b.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, crosstab_list, load_meps, svy_mean, svy_total  # noqa: E402

pd.set_option("display.width", 200)

print("AHRQ MEPS DATA USERS WORKSHOP -- 2018")
print("EXERCISE5: CALCULATE EXPENDITURES FOR ALL EVENTS ASSOCIATED WITH A CONDITION (DIABETES)\n")

GEZERO = [(0, np.inf, "GE 0")]
PAY = ["SF", "MR", "MD", "PV", "VA", "TR", "OF", "SL", "WC", "OR", "OU", "OT"]
EXPVARS = ["TOTEXP"] + PAY

# 1) PULL OUT CONDITIONS WITH DIABETES (CCS CODE='049', '050') FROM 2015
#    CONDITION PUF - HC180 ----------------------------------------------------

H180 = load_meps("h180", columns=["DUPERSID", "CONDIDX", "CCCODEX"])
DIAB = H180[H180["CCCODEX"].isin(["049", "050"])]
print("CHECK CCS CODES")
print(pd.crosstab(DIAB["CCCODEX"], "COUNT"), "\n")

# 2) GET EVENT ID FOR THE DIABETIC CONDITIONS FROM CONDITION-EVENT LINK FILE --

H178IF1 = load_meps("h178if1", columns=["CONDIDX", "EVNTIDX"])
DIAB2 = DIAB.merge(H178IF1, on="CONDIDX", how="inner")
print("SAMPLE DUMP FOR CONDITION-EVENT LINK FILE")
print(DIAB2.head(20).to_string(index=False), "\n")

# 3) DELETE DUPLICATE CASES PER EVENT (PROC SORT NODUPKEY BY EVNTIDX) ---------

DIAB3 = DIAB2[["DUPERSID", "EVNTIDX"]].sort_values("EVNTIDX").drop_duplicates("EVNTIDX")
print("SAMPLE DUMP AFTER DUPLICATE CASES ARE DELETED")
print(DIAB3.head(30).to_string(index=False), "\n")

# 4) SUM UP PMED PURCHASE-LEVEL DATA TO EVENT-LEVEL ---------------------------

rx_cols = ["RXXP15X"] + [f"RX{p}15X" for p in PAY]
H178A = load_meps("h178a", columns=["LINKIDX"] + rx_cols)
PMED2 = H178A.rename(columns={"LINKIDX": "EVNTIDX"}).groupby("EVNTIDX")[rx_cols].sum().reset_index()


# 5) ALIGN EXP VARIABLES IN DIFFERENT EVENTS WITH THE SAME NAMES ---------------


def align(df, prefixes, total):
    """SF = <prefix>SF15X (+ <prefix2>SF15X ...); TOTEXP = total; keep TOTEXP >= 0."""
    out = pd.DataFrame({"EVNTIDX": df["EVNTIDX"]})
    for p in PAY:
        out[p] = sum(df[f"{pre}{p}15X"] for pre in prefixes)
    out["TOTEXP"] = df[total]
    return out[out["TOTEXP"] >= 0]


def event_file(name, prefixes, total):
    cols = ["EVNTIDX", total] + [f"{pre}{p}15X" for pre in prefixes for p in PAY]
    return align(load_meps(name, columns=cols), prefixes, total)


PMED3 = align(PMED2, ["RX"], "RXXP15X")
OB = event_file("h178g", ["OB"], "OBXP15X")
EROM = event_file("h178e", ["ERF", "ERD"], "ERXP15X")
IPAT = event_file("h178d", ["IPF", "IPD"], "IPXP15X")
HVIS = event_file("h178h", ["HH"], "HHXP15X")
OPAT = event_file("h178f", ["OPF", "OPD"], "OPXP15X")

# 6) COMBINE ALL EVENTS INTO ONE DATASET --------------------------------------

ALLEVENT = pd.concat(
    [
        OB.assign(EVNTYP="AMBU"),
        EROM.assign(EVNTYP="EROM"),
        IPAT.assign(EVNTYP="IPAT"),
        HVIS.assign(EVNTYP="HVIS"),
        OPAT.assign(EVNTYP="AMBU"),
        PMED3.assign(EVNTYP="PMED"),
    ],
    ignore_index=True,
).sort_values("EVNTIDX", kind="stable")

print("ALL EVENTS ARE COMBINED INTO ONE FILE")
print(crosstab_list(ALLEVENT, ["EVNTYP"]))
for v in EXPVARS:
    print(crosstab_list(ALLEVENT.assign(**{v: apply_format(ALLEVENT[v], GEZERO)}), [v]))
print(ALLEVENT.head(20).to_string(index=False), "\n")

# 7) SUBSET EVENTS TO THOSE ONLY WITH DIABETES --------------------------------

DIAB4 = DIAB3.merge(ALLEVENT, on="EVNTIDX", how="inner")

# 8) CALCULATE ESTIMATES ON EXPENDITURES AND USE, ALL TYPES OF SERVICE ---------

ALL = DIAB4.groupby("DUPERSID")[EXPVARS].sum().reset_index()
H181 = load_meps("h181", columns=["DUPERSID", "VARPSU", "VARSTR", "PERWT15F"])

FY1 = H181.merge(ALL, on="DUPERSID", how="left", indicator=True)
# SUB = 1: PERSONS WHO HAVE AT LEAST 1 EVENT ASSOCIATED WITH DIABETES
FY1["SUB"] = np.where(FY1.pop("_merge") == "both", 1, 2)
FY1[EXPVARS] = FY1[EXPVARS].fillna(0)  # PERSONS WITHOUT EVENTS WITH DIABETES
FY1 = FY1[FY1["PERWT15F"] > 0]

design = dict(weight="PERWT15F", strata="VARSTR", psu="VARPSU")
OUT1 = svy_total(FY1, EXPVARS, domain="SUB", **design).merge(
    svy_mean(FY1, EXPVARS, domain="SUB", **design), on=["SUB", "variable", "n"], suffixes=("_sum", "_mean")
)
print("ESTIMATES ON USE AND EXPENDITURES FOR ALL EVENTS ASSOCIATED WITH DIABETES, 2015")
print(OUT1[OUT1["SUB"] == 1].drop(columns="SUB").to_string(index=False), "\n")

# 9) CALCULATE ESTIMATES ON EXPENDITURES AND USE BY TYPE OF SERVICE -----------

TOS2 = (
    DIAB4.groupby(["DUPERSID", "EVNTYP"])
    .agg(N_VISITS=("EVNTIDX", "size"), **{v: (v, "sum") for v in EXPVARS})
    .reset_index()
)
print("SAMPLE DUMP AFTER DATA IS SUMMED UP TO PERSON-EVENT TYPE-LEVEL")
print(TOS2.head(20).to_string(index=False), "\n")

FYTOS = H181.merge(TOS2, on="DUPERSID", how="left", indicator=True)
FYTOS["SUB"] = np.where(FYTOS.pop("_merge") == "both", 1, 2)
FYTOS["EVNTYP"] = FYTOS["EVNTYP"].fillna("NA")
FYTOS[["N_VISITS"] + EXPVARS] = FYTOS[["N_VISITS"] + EXPVARS].fillna(0)
FYTOS = FYTOS[FYTOS["PERWT15F"] > 0]

vars_ = ["N_VISITS"] + EXPVARS
dom = ["SUB", "EVNTYP"]
OUT2 = svy_total(FYTOS, vars_, domain=dom, **design).merge(
    svy_mean(FYTOS, vars_, domain=dom, **design), on=dom + ["variable", "n"], suffixes=("_sum", "_mean")
)
OUT2 = OUT2[OUT2["SUB"] == 1].drop(columns="SUB").sort_values("EVNTYP", kind="stable")
print("ESTIMATES ON USE AND EXPENDITURES FOR EVENTS ASSOCIATED WITH DIABETES, BY TYPE OF SERVICE, 2015")
for evntyp, grp in OUT2.groupby("EVNTYP"):
    print(f"\nEVNTYP={evntyp}")
    print(grp.drop(columns="EVNTYP").to_string(index=False))
