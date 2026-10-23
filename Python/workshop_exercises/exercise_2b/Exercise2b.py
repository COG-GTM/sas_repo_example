"""Python port of SAS/workshop_exercises/exercise_2b/Exercise2b.sas

/

PURPOSE:    THIS PROGRAM GENERATES SELECTED ESTIMATES FOR A 2016 VERSION OF THE Purchases and Expenses for Narcotic analgesics or Narcotic analgesic combos


    (1) FIGURE 1: TOTAL EXPENSE FOR Narcotic analgesics or Narcotic analgesic combos

    (2) FIGURE 2: TOTAL NUMBER OF PURCHASES OF Narcotic analgesics or Narcotic analgesic combos

    (3) FIGURE 3: TOTAL NUMBER OF PERSONS PURCHASING ONE OR MORE Narcotic analgesics or Narcotic analgesic combos

    (4) FIGURE 4: AVERAGE TOTAL, OUT OF POCKET, AND THIRD PARTY PAYER EXPENSE
                  FOR Narcotic analgesics or Narcotic analgesic combos PER PERSON WITH AN Narcotic analgesics or Narcotic analgesic combos MEDICINE PURCHASE

INPUT FILES:  (1) <MEPS_DIR>/SAS/DATA/H1192.SAS7BDAT (2016 FULL-YEAR CONSOLIDATED PUF)
              (2) <MEPS_DIR>/SAS/DATA/H188A.SAS7BDAT (2016 PRESCRIBED MEDICINES PUF)


IMPORTANT NOTES: Use the next 6 lines of code, if you want to specify an alternative destination for SAS log and
SAS procedure output.

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# Select the drug class from the prescribed medicines file (SAS: DATA DRUG; SET ...; IF ...)
RX = read_meps("h188a")
DRUG = RX[RX.TC1S1_1.isin([60, 191])].copy()
print(DRUG[["DUPERSID", "RXRECIDX", "LINKIDX", "RXXP16X", "RXSF16X"]].head(30))

# Person-level totals (SAS: PROC SUMMARY NWAY; CLASS DUPERSID; OUTPUT sum= ; _FREQ_)
PERDRUG = DRUG.groupby("DUPERSID").agg(N_PHRCHASE=("RXXP16X", "size"), TOT=("RXXP16X", "sum"),
                                       OOP=("RXSF16X", "sum")).reset_index()
PERDRUG["THIRD_PAYER"] = PERDRUG.TOT - PERDRUG.OOP

# Merge with the FYC file, keeping all FYC persons (SAS: MERGE ... (IN=AA) (IN=BB); IF AA)
FYC = read_meps("h192", ["DUPERSID", "VARSTR", "VARPSU", "PERWT16F"])
FY = FYC.merge(PERDRUG, on="DUPERSID", how="left", indicator=True)
FY["SUB"] = np.where(FY._merge == "both", 1, 2)  # 1 = persons with 1+ purchase of narcotic analgesics or narcotic analgesic combos
FY[["N_PHRCHASE", "TOT", "OOP", "THIRD_PAYER"]] = FY[["N_PHRCHASE", "TOT", "OOP", "THIRD_PAYER"]].fillna(0)
FY = FY.drop(columns="_merge")

# QC (PROC FREQ ... / LIST MISSING)
print(freq_list(FY.assign(TOT=sas_format(FY.TOT, {0: "0", slice(1e-12, None): ">0"})), ["SUB", "TOT"]))

# Estimates for persons with 1+ purchase: DOMAIN SUB('1') (all records kept in the design)
out = surveymeans(FY, ["TOT", "N_PHRCHASE", "OOP", "THIRD_PAYER"], weight="PERWT16F", strata="VARSTR",
                  cluster="VARPSU", domain="SUB")
show("Persons with 1+ purchase of narcotic analgesics or narcotic analgesic combos (SUB = 1)",
     out[out.SUB == 1][["VarName", "N", "SumWgt", "Mean", "StdErr", "Sum", "StdDev"]])
