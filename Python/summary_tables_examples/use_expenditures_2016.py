# -----------------------------------------------------------------------------
# Example code to replicate estimates from the MEPS-HC Data Tools summary tables
#
# Use, expenditures, and population, 2016
#
# Expenditures by event type and source of payment (SOP)
#  - Total expenditures
#  - Mean expenditure per person
#  - Mean out-of-pocket (SLF) payment per person with an out-of-pocket expense
#
# Selected event types:
#  - Office-based medical visits (OBV)
#  - Office-based physician visits (OBD)
#  - Outpatient visits (OPT)
#  - Outpatient physician visits (OPV)
#
# Input file: C:\MEPS\h192.ssp (2016 full-year consolidated)
#
# Python port of SAS/summary_tables_examples/use_expenditures_2016.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import MEPS_DIR, read_meps, svy_mean, svy_total  # noqa: E402

pd.set_option("display.width", 200)

# Load FYC file (SAS transport file) ------------------------------------------

h192 = read_meps(f"{MEPS_DIR}/h192.ssp")

# Aggregate payment sources ---------------------------------------------------
#
#  Notes:
#   - For 1996-1999: TRICARE label is CHM (changed to TRI in 2000)
#
#   - For 1996-2006: combined facility + SBD variables for hospital-type events
#      are not on PUF
#
#   - Starting in 2019, 'Other public' (OPU) and 'Other private' (OPR) are
#      dropped from the files
#
#  PTR = Private (PRV) + TRICARE (TRI)
#
#  OTZ = other federal (OFD)  + State/local (STL) + other private (OPR) +
#         other public (OPU)  + other unclassified sources (OSR) +
#         worker's comp (WCP) + Veteran's (VA)

FYC = h192.copy()
otz = ["OFD", "STL", "OPR", "OPU", "OSR", "WCP", "VA"]
# OBV: office-based visits; OBD: office-based physician visits;
# OPT: outpatient visits (facility + SBD expenses);
# OPV / OPS: outpatient physician visits (facility / SBD expense)
#  - For 1996-2006: combined facility + SBD variables are not on PUF
for ev in ("OBV", "OBD", "OPT", "OPV", "OPS"):
    FYC[f"{ev}PTR"] = FYC[f"{ev}PRV16"] + FYC[f"{ev}TRI16"]
    FYC[f"{ev}OTZ"] = FYC[[f"{ev}{s}16" for s in otz]].sum(axis=1, min_count=len(otz))

# Combine facility and SBD expenses for hospital-type events
#  Note: for 1996-2006, also need to create OPT*** = OPF*** + OPD***
FYC["OPpSLF"] = FYC["OPVSLF16"] + FYC["OPSSLF16"]  # out-of-pocket payments
FYC["OPpMCR"] = FYC["OPVMCR16"] + FYC["OPSMCR16"]  # Medicare
FYC["OPpMCD"] = FYC["OPVMCD16"] + FYC["OPSMCD16"]  # Medicaid
FYC["OPpPTR"] = FYC["OPVPTR"] + FYC["OPSPTR"]  # private insurance (including TRICARE)
FYC["OPpOTZ"] = FYC["OPVOTZ"] + FYC["OPSOTZ"]  # other sources of payment

# Define domains for persons with out-of-pocket expense
FYC["has_OBVSLF"] = (FYC["OBVSLF16"] > 0).astype(int)
FYC["has_OBDSLF"] = (FYC["OBDSLF16"] > 0).astype(int)
FYC["has_OPTSLF"] = (FYC["OPTSLF16"] > 0).astype(int)
FYC["has_OPpSLF"] = (FYC["OPpSLF"] > 0).astype(int)

event = {
    "OBV": "Office-based visits",
    "OBD": "Office-based physician visits",
    "OPT": "Outpatient visits",
    "OPp": "Outpatient physician visits",
}
sop = {"SLF": "Out-of-pocket", "PTR": "Private", "MCR": "Medicare", "MCD": "Medicaid", "OTZ": "Other"}

# Calculate estimates using survey procedures ---------------------------------

design = dict(weight="PERWT16F", strata="VARSTR", psu="VARPSU")
variables = [
    f"{ev}{s}" if (ev == "OPp" or s in ("PTR", "OTZ")) else f"{ev}{s}16" for ev in event for s in sop
]
totals = svy_total(FYC, variables, **design)
means = svy_mean(FYC, variables, **design)
out = totals.merge(means, on="variable", suffixes=("_sum", "_mean"))

# Format output
out["event"] = out["variable"].str[:3].map(event)
out["SOP"] = out["variable"].str[3:6].map(sop)

# Total expenditures and mean exp per person, by event type and source of payment
print("Total expenditures and mean expenditure per person, by event type and source of payment")
print(
    out[["event", "SOP", "estimate_sum", "se_sum", "estimate_mean", "se_mean"]]
    .rename(columns={"estimate_sum": "Total expenditures", "estimate_mean": "Mean expenditure per person"})
    .to_string(index=False)
)

# Mean expenditure per person with expense
#  - Mean out-of-pocket expense per person with an out-of-pocket expense
for title, dom, var in (
    ("office-based visits", "has_OBVSLF", "OBVSLF16"),
    ("office-based phys. visits", "has_OBDSLF", "OBDSLF16"),
    ("outpatient visits", "has_OPTSLF", "OPTSLF16"),
    ("outpatient phys. visits", "has_OPpSLF", "OPpSLF"),
):
    res = svy_mean(FYC, var, domain=dom, **design)
    print(f"\n{title}")
    print(res[res[dom] == 1].to_string(index=False))
