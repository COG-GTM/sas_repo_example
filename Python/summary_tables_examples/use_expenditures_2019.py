# -----------------------------------------------------------------------------
# Example code to replicate estimates from the MEPS-HC Data Tools summary tables
#
# Use, expenditures, and population, 2019
#
# Expenditures by event type and source of payment (SOP)
#  - Mean expenditure per person
#
# Selected event types:
#  - All event types (TOT)
#  - Emergency room visits (ERT)
#  - Inpatient stays (IPT)
#
# Input file: C:\MEPS\h216.sas7bdat (2019 full-year consolidated)
#
# Python port of SAS/summary_tables_examples/use_expenditures_2019.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import MEPS_DIR, read_meps, svy_mean  # noqa: E402

pd.set_option("display.width", 200)

# Load FYC file ---------------------------------------------------------------

FYC = read_meps(f"{MEPS_DIR}/h216.sas7bdat")

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
#  OTZ = other federal (OFD)  + State/local (STL) +
#         other unclassified sources (OSR) +
#         worker's comp (WCP) + Veteran's (VA)

for ev in ("TOT", "ERT", "IPT"):  # All event types, ER visits, Inpatient stays
    FYC[f"{ev}OTZ"] = FYC[[f"{ev}{s}19" for s in ("OFD", "STL", "OSR", "WCP", "VA")]].sum(
        axis=1, min_count=5
    )

event = {"TOT": "Any event", "ERT": "Emergency room visits", "IPT": "Inpatient stays"}
sop = {
    "EXP": "Any source",
    "SLF": "Out-of-pocket",
    "PTR": "Private",
    "MCR": "Medicare",
    "MCD": "Medicaid",
    "OTZ": "Other",
}

# Calculate estimates using survey procedures ---------------------------------

variables = [f"{ev}{s}19" if s != "OTZ" else f"{ev}OTZ" for ev in event for s in sop]
out = svy_mean(FYC, variables, weight="PERWT19F", strata="VARSTR", psu="VARPSU")

# Format output
out["event"] = out["variable"].str[:3].map(event)
out["SOP"] = out["variable"].str[3:6].map(sop)

# Mean expenditure per person, by event type and source of payment
print("Mean expenditure per person, by event type and source of payment")
print(out[["event", "SOP", "estimate", "se"]].rename(columns={"estimate": "Mean"}).to_string(index=False))
