"""Python port of SAS/summary_tables_examples/use_events_2016.sas

Example code to replicate estimates from the MEPS-HC Data Tools summary tables

Use, expenditures, and population, 2016

Utilization and expenditures by event type and source of payment (SOP)
 - Total number of events
 - Mean expenditure per event, by source of payment
 - Mean events per person, for office-based visits

Selected event types:
 - Office-based medical visits (OBV)
 - Office-based physician visits (OBD)
 - Outpatient visits (OPT)
 - Outpatient physician visits (OPV)

Input files:
   - <MEPS_DIR>/h192.ssp (2016 full-year consolidated)
    - <MEPS_DIR>/h188f.ssp (2016 OP event file)
    - <MEPS_DIR>/h188g.ssp (2016 OB event file)

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# Load files (SAS: %load macro with PROC XCOPY) -----------------------------------
h192 = read_meps("h192")    # FYC file
h188f = read_meps("h188f")  # OP event file
h188g = read_meps("h188g")  # OB event file

# Define variables ----------------------------------------------------------------
#  Notes:
#   - For 1996-1999: TRICARE label is CHM (changed to TRI in 2000)
#   - For 1996-2006: combined facility + SBD variables for hospital-type events
#      are not on PUF
#   - Starting in 2019, 'Other public' (OPU) and 'Other private' (OPR) are
#      dropped from the files
#
#  PR = Private (PV) + TRICARE (TR)
#  OZ = other federal (OF) + State/local (SL) + other private (OR) +
#        other public (OU) + other unclassified sources (OT) +
#        Veterans (VA) + worker's comp (WC)

OB = h188g[h188g.OBXP16X >= 0].copy()  # Remove inapplicable events
OB["PR"] = OB.OBPV16X + OB.OBTR16X
OB["OZ"] = OB[["OBOF16X", "OBSL16X", "OBVA16X", "OBOT16X", "OBOR16X", "OBOU16X", "OBWC16X"]].sum(axis=1, min_count=7)
OB["count"] = 1  # counter for event totals
OB["phys_count"] = (OB.SEEDOC == 1).astype(int)

OP = h188f[h188f.OPXP16X >= 0].copy()  # Remove inapplicable events
OP["PR_fac"] = OP.OPFPV16X + OP.OPFTR16X
OP["PR_sbd"] = OP.OPDPV16X + OP.OPDTR16X
OP["OZ_fac"] = OP[["OPFOF16X", "OPFSL16X", "OPFOR16X", "OPFOU16X", "OPFVA16X", "OPFOT16X", "OPFWC16X"]].sum(axis=1, min_count=7)
OP["OZ_sbd"] = OP[["OPDOF16X", "OPDSL16X", "OPDOR16X", "OPDOU16X", "OPDVA16X", "OPDOT16X", "OPDWC16X"]].sum(axis=1, min_count=7)
OP["SF"] = OP.OPFSF16X + OP.OPDSF16X  # out-of-pocket payments
OP["MR"] = OP.OPFMR16X + OP.OPDMR16X  # Medicare
OP["MD"] = OP.OPFMD16X + OP.OPDMD16X  # Medicaid
OP["PR"] = OP.PR_fac + OP.PR_sbd      # private insurance (including TRICARE)
OP["OZ"] = OP.OZ_fac + OP.OZ_sbd      # other sources of payment
OP["count"] = 1

# Merge with FYC to retain all PSUs (SAS: MERGE ... BY DUPERSID, no IF -> full join)
FYC = h192[["DUPERSID", "VARSTR", "VARPSU", "PERWT16F"]]
design = ["VARSTR", "VARPSU", "PERWT16F"]
OB_FYC = OB.drop(columns=design).merge(FYC, on="DUPERSID", how="outer").sort_values("DUPERSID")
OP_FYC = OP.drop(columns=design).merge(FYC, on="DUPERSID", how="outer").sort_values("DUPERSID")

# Calculate estimates using survey procedures -------------------------------------
# Sources of payment (SOP) abbreviations:
#  - SF: Out-of-pocket
#  - PR: Private insurance, including TRICARE (PTR)
#  - MR: Medicare
#  - MD: Medicaid
#  - OZ: Other
for label, data, sops in [("Office-based", OB_FYC, ["OBSF16X", "PR", "OBMR16X", "OBMD16X", "OZ"]),
                          ("Outpatient", OP_FYC, ["SF", "PR", "MR", "MD", "OZ"])]:
    allv = ["count"] + sops
    stat = surveymeans(data, allv, weight="PERWT16F", strata="VARSTR", cluster="VARPSU")
    dom = surveymeans(data, allv, weight="PERWT16F", strata="VARSTR", cluster="VARPSU", domain="SEEDOC",
                      missing=True)
    dom = dom[dom.SEEDOC == 1]
    for title, t in [(f"{label} visits", stat), (f"{label} physician visits", dom)]:
        show(f"{title}: Total number of events", t[t.VarName == "count"][["Sum", "StdDev"]])
        show(f"{title}: Mean expenditure per event, by source of payment",
             t[t.VarName != "count"][["VarName", "Mean", "StdErr"]])

# Number of events per person (office-based) --------------------------------------
pers_OB = OB_FYC.groupby(["DUPERSID", "VARSTR", "VARPSU"], dropna=False).agg(
    n_events=("count", "sum"), n_phys_events=("phys_count", "sum"), PERWT16F=("PERWT16F", "mean")).reset_index()
pers_OB[["n_events", "n_phys_events"]] = pers_OB[["n_events", "n_phys_events"]].fillna(0)
show("Office-based visits per person",
     surveymeans(pers_OB, ["n_events", "n_phys_events"], weight="PERWT16F", strata="VARSTR", cluster="VARPSU")
     [["VarName", "Sum", "StdDev", "Mean", "StdErr"]])
