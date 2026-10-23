"""Python port of SAS/summary_tables_examples/use_expenditures_2019.sas

Example code to replicate estimates from the MEPS-HC Data Tools summary tables

Use, expenditures, and population, 2019

Expenditures by event type and source of payment (SOP)
 - Mean expenditure per person

Selected event types:
 - All event types (TOT)
 - Emergency room visits (ERT)
 - Inpatient stays (IPT)

Input file: <MEPS_DIR>/h216.sas7bdat (2019 full-year consolidated)

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path


sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# Load FYC file (SAS: LIBNAME mylib "C:/MEPS"; SET mylib.h216) ------------------
inFYC = read_meps("h216")

# Aggregate payment sources ------------------------------------------------------
#  Notes:
#   - For 1996-1999: TRICARE label is CHM (changed to TRI in 2000)
#   - For 1996-2006: combined facility + SBD variables for hospital-type events
#      are not on PUF
#   - Starting in 2019, 'Other public' (OPU) and 'Other private' (OPR) are
#      dropped from the files
#
#  OTZ = other federal (OFD) + State/local (STL) +
#         other unclassified sources (OSR) +
#         worker's comp (WCP) + Veteran's (VA)
FYC = inFYC.copy()
otz = ["OFD", "STL", "OSR", "WCP", "VA"]
for ev in ["TOT", "ERT", "IPT"]:
    FYC[f"{ev}OTZ"] = FYC[[f"{ev}{s}19" for s in otz]].sum(axis=1, min_count=len(otz))

# PROC FORMAT
event = {"TOT": "Any event", "ERT": "Emergency room visits", "IPT": "Inpatient stays"}
sop = {"EXP": "Any source", "SLF": "Out-of-pocket", "PTR": "Private", "MCR": "Medicare", "MCD": "Medicaid",
       "OTZ": "Other"}

# Calculate estimates using survey procedures -------------------------------------
# Mean expenditure per person, by event type and source of payment
vars_ = [f"{ev}{s}" for ev in ["TOT", "ERT", "IPT"] for s in ["EXP19", "SLF19", "PTR19", "MCR19", "MCD19", "OTZ"]]
out = surveymeans(FYC, vars_, weight="PERWT19F", strata="VARSTR", cluster="VARPSU")
out["event"] = sas_format(out.VarName.str[:3], event)
out["SOP"] = sas_format(out.VarName.str[3:6], sop)
show("Mean expenditure per person, by event type and source of payment", out[["event", "SOP", "N", "Mean", "StdErr"]])
