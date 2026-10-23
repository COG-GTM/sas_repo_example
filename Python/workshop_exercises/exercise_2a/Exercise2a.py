"""Python port of SAS/workshop_exercises/exercise_2a/Exercise2a.sas

/

PURPOSE:    THIS PROGRAM GENERATES SELECTED ESTIMATES FOR A 2015 VERSION OF THE
                MEPS STATISTICS BRIEF # 275: "Trends in Antipsychotics Purchases and Expenses for the U.S. Civilian
                                              Noninstitutionalized Population, 1997 and 2007"

    (1) FIGURE 1: TOTAL EXPENSE FOR ANTIPSYCHOTICS

    (2) FIGURE 2: TOTAL NUMBER OF PURCHASES OF ANTIPSYCHOTICS

    (3) FIGURE 3: TOTAL NUMBER OF PERSONS PURCHASING ONE OR MORE ANTIPSYCHOTICS

    (4) FIGURE 4: AVERAGE TOTAL, OUT OF POCKET, AND THIRD PARTY PAYER EXPENSE
                  FOR ANTIPSYCHOTICS PER PERSON WITH AN ANTIPSYCHOTIC MEDICINE PURCHASE

INPUT FILES:  (1) <MEPS_DIR>/SAS/DATA/H1181.SAS7BDAT (2015 FULL-YEAR CONSOLIDATED PUF)
              (2) <MEPS_DIR>/SAS/DATA/H178A.SAS7BDAT (2015 PRESCRIBED MEDICINES PUF)

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# Select the drug class from the prescribed medicines file (SAS: DATA DRUG; SET ...; IF ...)
RX = read_meps("h178a")
DRUG = RX[(RX.TC1 == 242) & (RX.TC1S1 == 251)].copy()
print(DRUG[["DUPERSID", "RXRECIDX", "LINKIDX", "RXXP15X", "RXSF15X"]].head(30))

# Person-level totals (SAS: PROC SUMMARY NWAY; CLASS DUPERSID; OUTPUT sum= ; _FREQ_)
PERDRUG = DRUG.groupby("DUPERSID").agg(N_PHRCHASE=("RXXP15X", "size"), TOT=("RXXP15X", "sum"),
                                       OOP=("RXSF15X", "sum")).reset_index()
PERDRUG["THIRD_PAYER"] = PERDRUG.TOT - PERDRUG.OOP

# Merge with the FYC file, keeping all FYC persons (SAS: MERGE ... (IN=AA) (IN=BB); IF AA)
FYC = read_meps("h181", ["DUPERSID", "VARSTR", "VARPSU", "PERWT15F"])
FY = FYC.merge(PERDRUG, on="DUPERSID", how="left", indicator=True)
FY["SUB"] = np.where(FY._merge == "both", 1, 2)  # 1 = persons with 1+ purchase of antipsychotic drugs
FY[["N_PHRCHASE", "TOT", "OOP", "THIRD_PAYER"]] = FY[["N_PHRCHASE", "TOT", "OOP", "THIRD_PAYER"]].fillna(0)
FY = FY.drop(columns="_merge")

# QC (PROC FREQ ... / LIST MISSING)
print(freq_list(FY.assign(TOT=sas_format(FY.TOT, {0: "0", slice(1e-12, None): ">0"})), ["SUB", "TOT"]))

# Estimates for persons with 1+ purchase: DOMAIN SUB('1') (all records kept in the design)
out = surveymeans(FY, ["TOT", "N_PHRCHASE", "OOP", "THIRD_PAYER"], weight="PERWT15F", strata="VARSTR",
                  cluster="VARPSU", domain="SUB")
show("Persons with 1+ purchase of antipsychotic drugs (SUB = 1)",
     out[out.SUB == 1][["VarName", "N", "SumWgt", "Mean", "StdErr", "Sum", "StdDev"]])
