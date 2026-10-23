"""Python port of SAS/workshop_exercises/exercise_3b/Exercise3b.sas

DESCRIPTION:  THIS PROGRAM ILLUSTRATES HOW TO CALCULATE EXPENDITURES FOR ALL EVENTS ASSOCIATED WITH A CONDITION

              THE CONDITION USED IN THIS EXERCISE IS DIABETES (CCS CODE=049 OR 050)

THE DEFINITION OF 61 CONDITIONS BASED ON CCS CODE

  1  Infectious diseases                                           : CCS CODE = 1-9
  2  Cancer                                                        : CCS CODE = 11-45
  3  Non-malignant neoplasm                                        : CCS CODE = 46, 47
  4  Thyroid disease                                               : CCS CODE = 48
  5  Diabetes mellitus                                             : CCS CODE = 49,50
  6  Other endocrine, nutritional & immune disorder                : CCS CODE = 51, 52, 54 - 58
  7  Hyperlipidemia                                                : CCS CODE = 53
  8  Anemia and other deficiencies                                 : CCS CODE = 59
  9  Hemorrhagic, coagulation, and disorders of White Blood cells  : CCS CODE = 60-64
  10 Mental disorders                                              : CCS CODE = 650-670
  11 CNS infection                                                 : CCS CODE = 76-78
  12 Hereditary, degenerative and other nervous system disorders   : CCS CODE = 79-81
  13 Paralysis                                                     : CCS CODE = 82
  14 Headache                                                      : CCS CODE = 84
  15 Epilepsy and convulsions                                      : CCS CODE = 83
  16 Coma, brain damage                                            : CCS CODE = 85
  17 Cataract                                                      : CCS CODE = 86
  18 Glaucoma                                                      : CCS CODE = 88
  19 Other eye disorders                                           : CCS CODE = 87, 89-91
  20 Otitis media                                                  : CCS CODE = 92
  21 Other CNS disorders                                           : CCS CODE = 93-95
  22 Hypertension                                                  : CCS CODE = 98,99
  23 Heart disease                                                 : CCS CODE = 96, 97, 100-108
  24 Cerebrovascular disease                                       : CCS CODE = 109-113
  25 Other circulatory conditions arteries, veins, and lymphatics  : CCS CODE = 114 -121
  26 Pneumonia                                                     : CCS CODE = 122
  27 Influenza                                                     : CCS CODE = 123
  28 Tonsillitis                                                   : CCS CODE = 124
  29 Acute Bronchitis and URI                                      : CCS CODE = 125 , 126
  30 COPD, asthma                                                  : CCS CODE = 127-134
  31 Intestinal infection                                          : CCS CODE = 135
  32 Disorders of teeth and jaws                                   : CCS CODE = 136
  33 Disorders of mouth and esophagus                              : CCS CODE = 137
  34 Disorders of the upper GI                                     : CCS CODE = 138-141
  35 Appendicitis                                                  : CCS CODE = 142
  36 Hernias                                                       : CCS CODE = 143
  37 Other stomach and intestinal disorders                        : CCS CODE = 144- 148
  38 Other GI                                                      : CCS CODE = 153-155
  39 Gallbladder, pancreatic, and liver disease                    : CCS CODE = 149-152
  40 Kidney Disease                                                : CCS CODE = 156-158, 160, 161
  41 Urinary tract infections                                      : CCS CODE = 159
  42 Other urinary                                                 : CCS CODE = 162,163
  43 Male genital disorders                                        : CCS CODE = 164-166
  44 Non-malignant breast disease                                  : CCS CODE = 167
  45 Female genital disorders, and contraception                   : CCS CODE = 168-176
  46 Complications of pregnancy and birth                          : CCS CODE = 177-195
  47 Normal birth/live born                                        : CCS CODE = 196, 218
  48 Skin disorders                                                : CCS CODE = 197-200
  49 Osteoarthritis and other non-traumatic joint disorders        : CCS CODE = 201-204
  50 Back problems                                                 : CCS CODE = 205
  51 Other bone and musculoskeletal  disease                       : CCS CODE = 206-209, 212
  52 Systemic lupus and connective tissues disorders               : CCS CODE = 210-211
  53 Congenital anomalies                                          : CCS CODE = 213-217
  54 Perinatal Conditions                                          : CCS CODE = 219-224
  55 Trauma-related disorders                                      : CCS CODE = 225-236, 239, 240, 244
  56 Complications of surgery or device                            : CCS CODE = 237, 238
  57 Poisoning by medical and non-medical substances               : CCS CODE = 241 - 243
  58 Residual Codes                                                : CCS CODE = 259
  59 Other care and screening                                      : CCS CODE = 10, 254-258
  60 Symptoms                                                      : CCS CODE = 245-252
  61 Allergic reactions                                            : CCS CODE = 253


INPUT FILES:  1) <MEPS_DIR>/SAS/DATA/H181.SAS7BDAT    (2015 FY PUF DATA)
              2) <MEPS_DIR>/SAS/DATA/H180.SAS7BDAT    (2015 CONDITION PUF DATA)
              3) <MEPS_DIR>/SAS/DATA/H178A.SAS7BDAT   (2015 PMED PUF DATA)
              4) <MEPS_DIR>/SAS/DATA/H178D.SAS7BDAT   (2015 INPATIENT VISITS PUF DATA)
              5) <MEPS_DIR>/SAS/DATA/H178E.SAS7BDAT   (2015 EROM VISITS PUF DATA)
              6) <MEPS_DIR>/SAS/DATA/H178F.SAS7BDAT   (2015 OUTPATIENT VISITS PUF DATA)
              7) <MEPS_DIR>/SAS/DATA/H178G.SASBDAT   (2015 OFFICE-BASED VISITS PUF DATA)
              8) <MEPS_DIR>/SAS/DATA/H178H.SAS7BDAT   (2015 HOME HEALTH PUF DATA)
              9) <MEPS_DIR>/SAS/DATA/H178IF1.SAS7BDAT  (2015 CONDITION-EVENT LINK PUF DATA)

;
OPTIONS LS=132 PS=79 NODATE FORMCHAR="|----|+|---+=|-//<>*" PAGENO=1;
FILENAME MYLOG "U:/MEPS/SAS/Exercise_5/Exercise5_log.TXT";
FILENAME MYPRINT "U:/MEPS/SAS/Exercise_5/Exercise5_OUTPUT.TXT";
PROC PRINTTO LOG=MYLOG PRINT=MYPRINT NEW;
RUN;
LIBNAME CDATA '<MEPS_DIR>/SAS/DATA';
LIBNAME CDATA "//programs.ahrq.local/programs/meps/AHRQ4_CY2/B_CFACT/BJ001DVK/Workshop_2018/SAS/Data";

TITLE1 'AHRQ MEPS DATA USERS WORKSHOP -- 2018';
TITLE2 'EXERCISE5.SAS: CALCULATE EXPENDITURES FOR ALL EVENTS ASSOCIATED WITH A CONDITION (DIABETES)';

PROC FORMAT;
  VALUE GTZERO
     0         = '0'
     0 <- HIGH = '>0'
               ;
  VALUE GEZERO
     0 - HIGH = 'GE 0' ;
RUN;

1) PULL OUT CONDITIONS WITH DIABETES (CCS CODE='049', '050') FROM 2015 CONDITION PUF - HC180

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

SOP = ["SF", "MR", "MD", "PV", "VA", "TR", "OF", "SL", "WC", "OR", "OU", "OT"]

# Diabetes conditions (CCS 049/050) linked to events through the CLNK file ---------
h180 = read_meps("h180")
DIAB = h180[h180.CCCODEX.isin(["049", "050"])][["DUPERSID", "CONDIDX", "CCCODEX"]]
print(freq_list(DIAB, ["CCCODEX"]))
CLNK = read_meps("h178if1", ["CONDIDX", "EVNTIDX"])
DIAB2 = DIAB.merge(CLNK, on="CONDIDX", how="inner")                        # IF AA AND BB
DIAB3 = DIAB2[["DUPERSID", "EVNTIDX"]].drop_duplicates("EVNTIDX")          # NODUPKEY BY EVNTIDX

# Prescribed medicines: sum fills to the LINKIDX (event) level ----------------------
rxcols = ["RXXP15X"] + [f"RX{s}15X" for s in SOP]
PMED2 = read_meps("h178a").rename(columns={"LINKIDX": "EVNTIDX"}).groupby("EVNTIDX")[rxcols].sum().reset_index()
PMED3 = PMED2.rename(columns={"RXXP15X": "TOTEXP", **{f"RX{s}15X": s for s in SOP}})


def event_file(name, prefix, two_part):
    """Event-level payments by source; hospital events add facility + SBD payments."""
    df = read_meps(name)
    out = pd.DataFrame({"EVNTIDX": df.EVNTIDX, "TOTEXP": df[f"{prefix}XP15X"]})
    for s in SOP:
        if two_part:
            out[s] = df[f"{prefix}F{s}15X"] + df[f"{prefix}D{s}15X"]
        else:
            out[s] = df[f"{prefix}{s}15X"]
    return out


OB = event_file("h178g", "OB", False)
EROM = event_file("h178e", "ER", True)
IPAT = event_file("h178d", "IP", True)
HVIS = event_file("h178h", "HH", False)
OPAT = event_file("h178f", "OP", True)

# Stack all events (SAS: SET ... (IN=); IF TOTEXP GE 0) ------------------------------
ALLEVENT = pd.concat([OB.assign(EVNTYP="AMBU"), EROM.assign(EVNTYP="EROM"), IPAT.assign(EVNTYP="IPAT"),
                      HVIS.assign(EVNTYP="HVIS"), OPAT.assign(EVNTYP="AMBU"), PMED3.assign(EVNTYP="PMED")],
                     ignore_index=True)
ALLEVENT = ALLEVENT[ALLEVENT.TOTEXP >= 0]
print(freq_list(ALLEVENT, ["EVNTYP"]))

# Diabetes-related events, summed to the person level --------------------------------
DIAB4 = DIAB3.merge(ALLEVENT, on="EVNTIDX", how="inner")
ALL = DIAB4.groupby("DUPERSID")[["TOTEXP"] + SOP].sum().reset_index()

FYC = read_meps("h181", ["DUPERSID", "VARPSU", "VARSTR", "PERWT15F"])
FY1 = FYC.merge(ALL, on="DUPERSID", how="left", indicator=True)
FY1["SUB"] = np.where(FY1._merge == "both", 1, 2)  # 2 = persons without diabetes events
FY1[["TOTEXP"] + SOP] = FY1[["TOTEXP"] + SOP].fillna(0)
FY1 = FY1[FY1.PERWT15F > 0].drop(columns="_merge")

out = surveymeans(FY1, ["TOTEXP"] + SOP, weight="PERWT15F", strata="VARSTR", cluster="VARPSU", domain="SUB")
show("Diabetes-related expenses per person with diabetes care, by source of payment (SUB = 1)",
     out[out.SUB == 1][["VarName", "N", "SumWgt", "Sum", "StdDev", "Mean", "StdErr"]])

# By type of service (SAS: PROC SUMMARY CLASS DUPERSID EVNTYP; _FREQ_ = N_VISITS) ----
TOS2 = DIAB4.groupby(["DUPERSID", "EVNTYP"]).agg(N_VISITS=("TOTEXP", "size"),
                                                 **{c: (c, "sum") for c in ["TOTEXP"] + SOP}).reset_index()
FYTOS = FYC.merge(TOS2, on="DUPERSID", how="left", indicator=True)
FYTOS["SUB"] = np.where(FYTOS._merge == "both", 1, 2)
FYTOS["EVNTYP"] = FYTOS.EVNTYP.fillna("NA")
FYTOS[["N_VISITS", "TOTEXP"] + SOP] = FYTOS[["N_VISITS", "TOTEXP"] + SOP].fillna(0)
FYTOS = FYTOS[FYTOS.PERWT15F > 0].drop(columns="_merge")

out2 = surveymeans(FYTOS, ["N_VISITS", "TOTEXP"] + SOP, weight="PERWT15F", strata="VARSTR", cluster="VARPSU",
                   domain=["SUB", "EVNTYP"])
out2 = out2[out2.SUB == 1].sort_values(["EVNTYP"], kind="stable")
for ev, t in out2.groupby("EVNTYP", sort=True):
    show(f"Diabetes-related events, type of service = {ev}", t[["VarName", "N", "SumWgt", "Sum", "StdDev", "Mean", "StdErr"]])
