"""Python port of SAS/summary_tables_examples/use_expenditures_2016.sas

Example code to replicate estimates from the MEPS-HC Data Tools summary tables

Use, expenditures, and population, 2016

Expenditures by event type and source of payment (SOP)
 - Total expenditures
 - Mean expenditure per person
 - Mean out-of-pocket (SLF) payment per person with an out-of-pocket expense

Selected event types:
 - Office-based medical visits (OBV)
 - Office-based physician visits (OBD)
 - Outpatient visits (OPT)
 - Outpatient physician visits (OPV)

Input file: <MEPS_DIR>/h192.ssp (2016 full-year consolidated)

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path


sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# Load FYC file (SAS: PROC XCOPY) -----------------------------------------------
h192 = read_meps("h192")

# Aggregate payment sources ------------------------------------------------------
#  Notes:
#   - For 1996-1999: TRICARE label is CHM (changed to TRI in 2000)
#   - For 1996-2006: combined facility + SBD variables for hospital-type events
#      are not on PUF
#   - Starting in 2019, 'Other public' (OPU) and 'Other private' (OPR) are
#      dropped from the files
#
#  PTR = Private (PRV) + TRICARE (TRI)
#  OTZ = other federal (OFD) + State/local (STL) + other private (OPR) +
#         other public (OPU) + other unclassified sources (OSR) +
#         worker's comp (WCP) + Veteran's (VA)
FYC = h192.copy()
otz = ["OFD", "STL", "OPR", "OPU", "OSR", "WCP", "VA"]
for ev in ["OBV", "OBD", "OPT", "OPV", "OPS"]:
    FYC[f"{ev}PTR"] = FYC[f"{ev}PRV16"] + FYC[f"{ev}TRI16"]
    FYC[f"{ev}OTZ"] = FYC[[f"{ev}{s}16" for s in otz]].sum(axis=1, min_count=len(otz))

# Outpatient physician visits = facility (OPV) + SBD (OPS) payments
FYC["OPpSLF"] = FYC.OPVSLF16 + FYC.OPSSLF16  # out-of-pocket payments
FYC["OPpMCR"] = FYC.OPVMCR16 + FYC.OPSMCR16  # Medicare
FYC["OPpMCD"] = FYC.OPVMCD16 + FYC.OPSMCD16  # Medicaid
FYC["OPpPTR"] = FYC.OPVPTR + FYC.OPSPTR      # private insurance (including TRICARE)
FYC["OPpOTZ"] = FYC.OPVOTZ + FYC.OPSOTZ      # other sources of payment

# Indicators for persons with out-of-pocket expense (domains for mean OOP)
for v, src in [("has_OBVSLF", "OBVSLF16"), ("has_OBDSLF", "OBDSLF16"), ("has_OPTSLF", "OPTSLF16"),
               ("has_OPpSLF", "OPpSLF")]:
    FYC[v] = (FYC[src] > 0).astype(int)

# PROC FORMAT
event = {"OBV": "Office-based visits", "OBD": "Office-based physician visits", "OPT": "Outpatient visits",
         "OPp": "Outpatient physician visits"}
sop = {"SLF": "Out-of-pocket", "PTR": "Private", "MCR": "Medicare", "MCD": "Medicaid", "OTZ": "Other"}

# Calculate estimates using survey procedures -------------------------------------
# Expenditures by event type and source of payment (sum = total, mean = mean per person)
vars_ = ["OBVSLF16", "OBVPTR", "OBVMCR16", "OBVMCD16", "OBVOTZ",   # office-based visits
         "OBDSLF16", "OBDPTR", "OBDMCR16", "OBDMCD16", "OBDOTZ",   # office-based phys. visits
         "OPTSLF16", "OPTPTR", "OPTMCR16", "OPTMCD16", "OPTOTZ",   # outpatient visits
         "OPpSLF", "OPpPTR", "OPpMCR", "OPpMCD", "OPpOTZ"]         # outpatient phys. visits
out = surveymeans(FYC, vars_, weight="PERWT16F", strata="VARSTR", cluster="VARPSU")
out["event"] = sas_format(out.VarName.str[:3], event)
out["SOP"] = sas_format(out.VarName.str[3:6], sop)
show("Expenditures by event type and source of payment", out[["event", "SOP", "N", "Sum", "StdDev", "Mean", "StdErr"]])

# Mean out-of-pocket expense per person with an out-of-pocket expense
for dom, v in [("has_OBVSLF", "OBVSLF16"), ("has_OBDSLF", "OBDSLF16"), ("has_OPTSLF", "OPTSLF16"),
               ("has_OPpSLF", "OPpSLF")]:
    r = surveymeans(FYC, v, weight="PERWT16F", strata="VARSTR", cluster="VARPSU", domain=dom)
    show(f"Mean {v} among persons with {dom} = 1", r[r[dom] == 1][[dom, "VarName", "N", "Mean", "StdErr"]])
