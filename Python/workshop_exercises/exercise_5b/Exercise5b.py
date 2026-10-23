"""Python port of SAS/workshop_exercises/exercise_5b/Exercise5b.sas

DESCRIPTION:  THIS PROGRAM ILLUSTRATES HOW TO CONSTRUCT INSURANCE STATUS VARIABLES FROM
              MONTHLY INSURANCE VARIABLES (see below) IN THE PERSON-LEVEL DATA

TRImm15X   Covered by TRICARE/CHAMPVA in mm (Ed)
MCRmm15    Covered by Medicare in mm
MCRmm15X   Covered by Medicare in mm (Ed)
MCDmm15    Covered by Medicaid or SCHIP in mm
MCDmm15X   Covered by Medicaid or SCHIP in mm  (Ed)
OPAmm15    Covered by Other Public A Ins in mm
OPBmm15    Covered by Other Public B Ins in mm
PUBmm15X   Covered by Any Public Ins in mm (Ed)
PEGmm15    Covered by Empl Union Ins in mm
PDKmm15    Coverer by Priv Ins (Source Unknown) in mm
PNGmm15    Covered by Nongroup Ins in mm
POGmm15    Covered by Other Group Ins in mm
PRSmm15    Covered by Self-Emp Ins in mm
POUmm15    Covered by Holder Outside of RU in mm
PRImm15    Covered by Private Ins in mm

where mm = JA-DE  (January - December)

INPUT FILE:   <MEPS_DIR>/SAS/DATA/H181.SAS7BDAT (2015 FY PUF DATA)

;
OPTIONS LS=160 PS=79 NODATE FORMCHAR="|----|+|---+=|-//<>*" PAGENO=1;
FILENAME MYLOG "U:/MEPS/SAS/Exercise_7/Exercise7_log.TXT";
FILENAME MYPRINT "U:/MEPS/SAS/Exercise_7/Exercise7_OUTPUT.TXT";
PROC PRINTTO LOG=MYLOG PRINT=MYPRINT NEW;
RUN;

LIBNAME CDATA '<MEPS_DIR>/SAS/DATA';
LIBNAME CDATA "//programs.ahrq.local/programs/meps/AHRQ4_CY2/B_CFACT/BJ001DVK/Workshop_2017/SAS/Data";

%LET YR=15;

TITLE1 '2018 AHRQ MEPS DATA USERS WORKSHOP';
TITLE2 "EXERCISE7.SAS: HOW TO CONSTRUCT INSURANCE STATUS VARIABLES, USING FY &YR DATA";

PROC FORMAT;
VALUE RACETHX
  1 = '1 HISPANIC'
  2 = '2 WHITE'
  3 = '3 BLACK'
  4 = '4 ASIAN'
  5 = '5 OTHER RACE'
  ;
RUN;


1) COUNT # OF MONTHS WITH INSURANCE

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

YR = "15"
MONTHS = ["JA", "FE", "MA", "AP", "MY", "JU", "JL", "AU", "SE", "OC", "NO", "DE"]
RACETHX = {1: "1 HISPANIC", 2: "2 WHITE", 3: "3 BLACK", 4: "4 ASIAN", 5: "5 OTHER RACE"}

FY1 = read_meps("h181")


def arr(prefix, suffix=""):
    """SAS ARRAY over the 12 monthly variables, e.g. PRIJA15 ... PRIDE15."""
    return FY1[[f"{prefix}{m}{YR}{suffix}" for m in MONTHS]].to_numpy()


PEG, POU, PDK, PNG, POG, PRS, PRX, PRI = (arr(p) for p in ["PEG", "POU", "PDK", "PNG", "POG", "PRS", "PRX", "PRI"])
INS, MCD, MCR, TRI = (arr(p, "X") for p in ["INS", "MCD", "MCR", "TRI"])
OPA, OPB = arr("OPA"), arr("OPB")

# Count months of each coverage type (SAS: DO I=1 TO 12; IF ... THEN X_N+1)
FY1["PRI_N"] = (PRI == 1).sum(axis=1)
FY1["INS_N"] = (INS == 1).sum(axis=1)
FY1["UNINS_N"] = (INS == 2).sum(axis=1)
FY1["MCD_N"] = (MCD == 1).sum(axis=1)
FY1["MCR_N"] = (MCR == 1).sum(axis=1)
FY1["TRI_N"] = (TRI == 1).sum(axis=1)
FY1["OPAB_N"] = ((OPA == 1) | (OPB == 1)).sum(axis=1)
FY1["GRP_N"] = ((PEG == 1) | (TRI == 1) | (POU == 1) | (PDK == 1)).sum(axis=1)
FY1["NG_N"] = ((PRX == 1) | (PNG == 1) | (POG == 1) | (PRS == 1)).sum(axis=1)
FY1["PUB_N"] = ((MCR == 1) | (MCD == 1) | (OPA == 1) | (OPB == 1)).sum(axis=1)
FY1["REF_N"] = (INS > 0).sum(axis=1)
for v in ["PRI_N", "INS_N", "UNINS_N", "MCD_N", "MCR_N", "TRI_N", "OPAB_N", "GRP_N", "NG_N", "PUB_N", "REF_N"]:
    print(freq_list(FY1, [v]))

FY2 = FY1
FY2["FULL_INSU"] = np.where(FY2.UNINS_N == 0, 1, 0)
FY2["GROUP_INS1"] = np.where(FY2.GRP_N > 0, 1, 0)
FY2["GROUP_INS2"] = np.where((FY2.GRP_N > 0) & (FY2.GRP_N == FY2.REF_N), 1, 0)
FY2["NG_INS"] = np.where(FY2.NG_N > 0, 1, 0)
print(freq_list(FY2, ["FULL_INSU", "UNINS_N"]))
print(freq_list(FY2, ["GROUP_INS2", "GRP_N", "REF_N"]))

out = surveymeans(FY2, ["FULL_INSU", "GROUP_INS1", "GROUP_INS2", "NG_INS"], weight="PERWT15F", strata="VARSTR",
                  cluster="VARPSU", domain="RACETHX")
out["RACETHX"] = sas_format(out.RACETHX, RACETHX)
show("Insurance coverage measures by race/ethnicity, 2015",
     out.sort_values(["RACETHX"], kind="stable")[["RACETHX", "VarName", "N", "SumWgt", "Mean", "StdErr", "Sum", "StdDev"]])
