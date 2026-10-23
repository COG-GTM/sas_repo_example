"""Python port of SAS/workshop_exercises/cond_pmed_2020.sas

----------------------------------------------------------------------------------------------------------------

MEPS-HC: Prescribed medicine utilization and expenditures for the treatment of hyperlipidemia

This example code shows how to link the MEPS-HC Medical Conditions file to the Prescribed Medicines file for data year
2020 in order to estimate the following:

National totals:
   - Total number of people w/ at least one PMED fill for hyperlipidemia (HL)
   - Total PMED fills for HL
   - Total PMED expenditures for HL

Per-person averages among people with at least one PMED fill for HL:
   - Avg PMED fills for HL, by sex and poverty (POVCAT20)
   - Avg PMED expenditures for HL, by sex and poverty (POVCAT20)

Input files:
  - h220a.sas7bdat        (2020 Prescribed Medicines file)
  - h222.sas7bdat         (2020 Conditions file)
  - h220if1.sas7bdat      (2020 CLNK: Condition-Event Link file)
  - h224.sas7bdat         (2020 Full-Year Consolidated file)

Resources:
  - CCSR codes:
    https://github.com/HHS-AHRQ/MEPS/blob/master/Quick_Reference_Guides/meps_ccsr_conditions.csv

  - MEPS-HC Public Use Files:
    https://meps.ahrq.gov/mepsweb/data_stats/download_data_files.jsp

  - MEPS-HC online data tools:
    https://datatools.ahrq.gov/meps-hc

----------------------------------------------------------------------------------------------------------------

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# Load datasets ---------------------------------------------------------------------
pmed20 = read_meps("h220a").rename(columns={"LINKIDX": "EVNTIDX"})  # LINKIDX -> EVNTIDX for merging
pmed20 = pmed20[["DUPERSID", "DRUGIDX", "RXRECIDX", "EVNTIDX", "RXDRGNAM", "RXXP20X"]]
cond20 = read_meps("h222", ["DUPERSID", "CONDIDX", "ICD10CDX", "CCSR1X", "CCSR2X", "CCSR3X"])
clnk20 = read_meps("h220if1")
fyc20 = read_meps("h224", ["DUPERSID", "AGELAST", "SEX", "POVCAT20", "CHOLDX", "PERWT20F", "VARPSU", "VARSTR"])

# Hyperlipidemia conditions (CCSR END010) ---------------------------------------------
hl = cond20[(cond20.CCSR1X == "END010") | (cond20.CCSR2X == "END010") | (cond20.CCSR3X == "END010")]
print(hl[hl.DUPERSID == "2320134102"])  # example person with 'duplicate' HL conditions

# Merge to CLNK (inner), then de-duplicate by event so PMEDs are not double-counted
clnk_hl = hl.merge(clnk20, on=["DUPERSID", "CONDIDX"], how="inner")
print(clnk_hl[clnk_hl.DUPERSID == "2320134102"])
clnk_hl_dedup = clnk_hl.drop_duplicates(["DUPERSID", "EVNTIDX"])
print(freq_list(clnk_hl_dedup, ["EVENTYPE"]))

hl_merged = clnk_hl_dedup.merge(pmed20, on=["DUPERSID", "EVNTIDX"], how="inner")
print(freq_list(hl_merged, ["EVENTYPE"]))
print(hl_merged.RXDRGNAM.value_counts().head(10))
hl_merged["HL_FILL"] = 1

# Fills and expenditures per person (SAS: PROC MEANS NWAY; CLASS DUPERSID; OUTPUT sum=)
drugs_by_pers = hl_merged.groupby("DUPERSID").agg(N_HL_FILLS=("HL_FILL", "sum"),
                                                  HL_DRUG_EXP=("RXXP20X", "sum")).reset_index()

# Keep everyone on the FYC (VARSTR/VARPSU needed for correct SEs) ---------------------
fyc_hl = fyc20.merge(drugs_by_pers, on="DUPERSID", how="left")
fyc_hl["HL_PMED_FLAG"] = np.where(fyc_hl.N_HL_FILLS > 0, 1, 0)
fyc_hl[["N_HL_FILLS", "HL_DRUG_EXP"]] = fyc_hl[["N_HL_FILLS", "HL_DRUG_EXP"]].fillna(0)
print(pd.crosstab(fyc_hl.CHOLDX, fyc_hl.HL_PMED_FLAG))
print(freq_list(fyc_hl, ["HL_PMED_FLAG"]))

# National totals: people with HL fills, number of fills, HL rx expenditures ---------
show("Totals", surveymeans(fyc_hl, ["HL_PMED_FLAG", "N_HL_FILLS", "HL_DRUG_EXP"], weight="PERWT20F")
     [["VarName", "Sum", "StdDev"]])

# Per-person averages among people with 1+ HL fill, overall / by sex / by poverty -----
# (DOMAIN hl_pmed_flag('1') hl_pmed_flag('1')*sex hl_pmed_flag('1')*povcat20)
for dom in (["HL_PMED_FLAG"], ["HL_PMED_FLAG", "SEX"], ["HL_PMED_FLAG", "POVCAT20"]):
    m = surveymeans(fyc_hl, ["N_HL_FILLS", "HL_DRUG_EXP"], weight="PERWT20F", domain=dom)
    show(f"Domain {'*'.join(dom)}", m[m.HL_PMED_FLAG == 1][dom + ["VarName", "N", "Mean", "StdErr"]])
