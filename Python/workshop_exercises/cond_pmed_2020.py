# -----------------------------------------------------------------------------
# MEPS-HC: Prescribed medicine utilization and expenditures for the treatment
# of hyperlipidemia
#
# This example code shows how to link the MEPS-HC Medical Conditions file to the
# Prescribed Medicines file for data year 2020 in order to estimate the
# following:
#
# National totals:
#    - Total number of people w/ at least one PMED fill for hyperlipidemia (HL)
#    - Total PMED fills for HL
#    - Total PMED expenditures for HL
#
# Per-person averages among people with at least one PMED fill for HL:
#    - Avg PMED fills for HL, by sex and poverty (POVCAT20)
#    - Avg PMED expenditures for HL, by sex and poverty (POVCAT20)
#
# Input files:
#   - h220a.sas7bdat        (2020 Prescribed Medicines file)
#   - h222.sas7bdat         (2020 Conditions file)
#   - h220if1.sas7bdat      (2020 CLNK: Condition-Event Link file)
#   - h224.sas7bdat         (2020 Full-Year Consolidated file)
#
# Resources:
#   - CCSR codes:
#     https://github.com/HHS-AHRQ/MEPS/blob/master/Quick_Reference_Guides/meps_ccsr_conditions.csv
#   - MEPS-HC Public Use Files:
#     https://meps.ahrq.gov/mepsweb/data_stats/download_data_files.jsp
#   - MEPS-HC online data tools:
#     https://datatools.ahrq.gov/meps-hc
#
# Python port of SAS/workshop_exercises/cond_pmed_2020.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import load_meps, svy_mean, svy_total  # noqa: E402

pd.set_option("display.width", 200)

# Read in data files ----------------------------------------------------------

# PMED file (record = rx fill or refill for a person); rename LINKIDX to EVNTIDX
pmed20 = load_meps("h220a", columns=["DUPERSID", "DRUGIDX", "RXRECIDX", "LINKIDX", "RXDRGNAM", "RXXP20X"])
pmed20 = pmed20.rename(columns={"LINKIDX": "EVNTIDX"})
# Conditions file (record = medical condition for a person)
cond20 = load_meps("h222", columns=["DUPERSID", "CONDIDX", "ICD10CDX", "CCSR1X", "CCSR2X", "CCSR3X"])
# Conditions-event link file (crosswalk between conditions and medical events)
clnk20 = load_meps("h220if1")
# Full-year consolidated (person-level) file (record = MEPS sample member)
fyc20 = load_meps("h224", columns=["DUPERSID", "AGELAST", "SEX", "POVCAT20", "CHOLDX", "PERWT20F", "VARPSU", "VARSTR"])

# Prepare data for estimation -------------------------------------------------

# Subset conditions file to only hyperlipidemia records (any CCSR = "END010")
hl = cond20[(cond20[["CCSR1X", "CCSR2X", "CCSR3X"]] == "END010").any(axis=1)]

# Example of someone with 'duplicate' hyperlipidemia conditions (different
# CONDIDXs collapsing to the same 3-digit ICD10). Not de-duplicated yet.
print(hl[hl["DUPERSID"] == "2320134102"].to_string(index=False), "\n")

# Get EVNTIDX values for hyperlipidemia records from CLNK file
clnk_hl = hl.merge(clnk20, on=["DUPERSID", "CONDIDX"], how="inner")
print(clnk_hl[clnk_hl["DUPERSID"] == "2320134102"].to_string(index=False), "\n")

# De-duplicate by EVNTIDX so the same PMEDs aren't double-counted
clnk_hl_dedup = clnk_hl.sort_values(["DUPERSID", "EVNTIDX"], kind="stable").drop_duplicates(["DUPERSID", "EVNTIDX"])
print(clnk_hl_dedup[clnk_hl_dedup["DUPERSID"] == "2320134102"].to_string(index=False), "\n")
print(pd.crosstab(clnk_hl_dedup["EVENTYPE"], "COUNT"), "\n")

# Get PMED events (and NOT other event types) linked to hyperlipidemia
hl_merged = clnk_hl_dedup.merge(pmed20, on=["DUPERSID", "EVNTIDX"], how="inner")
print("QC: all events should have EVENTYPE = 8 (PMED)")
print(pd.crosstab(hl_merged["EVENTYPE"], "COUNT"), "\n")
print("QC: top PMEDs (by unweighted # fills) for hyperlipidemia")
print(hl_merged["RXDRGNAM"].value_counts().head(10).to_string(), "\n")

# Dummy variable for each unique fill, then roll up to person level
hl_merged["HL_FILL"] = 1
drugs_by_pers = (
    hl_merged.groupby("DUPERSID")
    .agg(N_HL_FILLS=("HL_FILL", "sum"), HL_DRUG_EXP=("RXXP20X", "sum"))
    .reset_index()
)

# Merge person-level totals back to FYC (keep everyone for correct SEs!)
fyc_hl = fyc20.merge(drugs_by_pers, on="DUPERSID", how="left")
fyc_hl["HL_PMED_FLAG"] = (fyc_hl["N_HL_FILLS"] > 0).astype(int)
# Missings caused by merging are true zeroes
fyc_hl[["N_HL_FILLS", "HL_DRUG_EXP"]] = fyc_hl[["N_HL_FILLS", "HL_DRUG_EXP"]].fillna(0)

print("QC: CHOLDX by HL_PMED_FLAG")
print(pd.crosstab(fyc_hl["CHOLDX"], fyc_hl["HL_PMED_FLAG"]), "\n")
print(f"QC: HL_PMED_FLAG = 1 count: {fyc_hl['HL_PMED_FLAG'].sum()} (rows in drugs_by_pers: {len(drugs_by_pers)})")
bad = fyc_hl[(fyc_hl["HL_PMED_FLAG"] == 0) & ((fyc_hl["HL_DRUG_EXP"] > 0) | (fyc_hl["N_HL_FILLS"] > 0))]
print(f"QC: records with flag = 0 but fills/exp > 0: {len(bad)}\n")

# ESTIMATION ------------------------------------------------------------------
design = dict(weight="PERWT20F", strata="VARSTR", psu="VARPSU")

# National totals
est_totals = svy_total(fyc_hl, ["HL_PMED_FLAG", "N_HL_FILLS", "HL_DRUG_EXP"], **design)
print("National totals")
print(est_totals.to_string(index=False), "\n")

# Per-person averages for people with 1+ PMED fill for hyperlipidemia
#  DOMAIN hl_pmed_flag('1') hl_pmed_flag('1')*sex hl_pmed_flag('1')*povcat20
print("Per-person averages among people with 1+ PMED fill for hyperlipidemia")
for dom in (["HL_PMED_FLAG"], ["HL_PMED_FLAG", "SEX"], ["HL_PMED_FLAG", "POVCAT20"]):
    est = svy_mean(fyc_hl, ["N_HL_FILLS", "HL_DRUG_EXP"], domain=dom, **design)
    print(est[est["HL_PMED_FLAG"] == 1].to_string(index=False), "\n")
