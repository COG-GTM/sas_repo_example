"""Python port of SAS/workshop_exercises/exercise_2c/Exercise2c.sas

This program generates National Totals and Per-person Averages for Narcotic
 analgesics and Narcotic analgesic combos care for the U.S. civilian
 non-institutionalized population (2018), including:
  - Number of purchases (fills)
  - Total expenditures
  - Out-of-pocket payments
  - Third-party payments

 Input files:
    - 2018 Prescribed medicines file
    - 2018 Full-year consolidated file

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# Select the drug class from the prescribed medicines file (SAS: DATA DRUG; SET ...; IF ...)
RX = read_meps("h206a")
DRUG = RX[RX.TC1S1_1.isin([60, 191])].copy()
print(DRUG[["DUPERSID", "RXRECIDX", "LINKIDX", "RXXP18X", "RXSF18X"]].head(30))

# Person-level totals (SAS: PROC SUMMARY NWAY; CLASS DUPERSID; OUTPUT sum= ; _FREQ_)
PERDRUG = DRUG.groupby("DUPERSID").agg(N_PHRCHASE=("RXXP18X", "size"), TOT=("RXXP18X", "sum"),
                                       OOP=("RXSF18X", "sum")).reset_index()
PERDRUG["THIRD_PAYER"] = PERDRUG.TOT - PERDRUG.OOP

# Merge with the FYC file, keeping all FYC persons (SAS: MERGE ... (IN=AA) (IN=BB); IF AA)
FYC = read_meps("h209", ["DUPERSID", "VARSTR", "VARPSU", "PERWT18F"])
FY = FYC.merge(PERDRUG, on="DUPERSID", how="left", indicator=True)
FY["SUBPOP"] = np.where(FY._merge == "both", 1, 2)  # 1 = persons with 1+ purchase of narcotic analgesics or narcotic analgesic combos
FY[["N_PHRCHASE", "TOT", "OOP", "THIRD_PAYER"]] = FY[["N_PHRCHASE", "TOT", "OOP", "THIRD_PAYER"]].fillna(0)
FY = FY.drop(columns="_merge")

# QC (PROC FREQ ... / LIST MISSING)
print(freq_list(FY.assign(TOT=sas_format(FY.TOT, {0: "0", slice(1e-12, None): ">0"})), ["SUBPOP", "TOT"]))

# Estimates for persons with 1+ purchase: DOMAIN SUBPOP('1') (all records kept in the design)
out = surveymeans(FY, ["TOT", "N_PHRCHASE", "OOP", "THIRD_PAYER"], weight="PERWT18F", strata="VARSTR",
                  cluster="VARPSU", domain="SUBPOP")
show("Persons with 1+ purchase of narcotic analgesics or narcotic analgesic combos (SUBPOP = 1)",
     out[out.SUBPOP == 1][["VarName", "N", "SumWgt", "Mean", "StdErr", "Sum", "StdDev"]])
