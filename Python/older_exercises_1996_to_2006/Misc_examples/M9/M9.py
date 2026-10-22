# --------------------------------------------------------------------
# PROGRAM:       C:\MEPS\PROG\M9.SAS
#
# DESCRIPTION:   THIS EXAMPLE USES THE 2005 FULL-YEAR FILE TO OUTPUT
#                DESCRIPTIVE STATISTICS SHOWING HEALTH INSURANCE
#                STATUS AND HEALTHCARE UTILIZATION.
#                TYPES OF EVENTS USED IN THIS PROGRAM:
#                PRESCRIBED MEDICINE USE (RXTOT05)
#                OFFICE-BASED VISITS (OBTOT05)
#                EMERGENCY DEPT. VISITS (ERTOT05)
#
# INPUT FILE:    C:\MEPS\DATA\H97.SAS7BDAT (2005 FULL-YEAR DATA FILE)
#
# Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M9/M9.sas
# --------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, load_meps, svy_mean, svy_total  # noqa: E402

pd.set_option("display.width", 200)

YESNO = {-9: "-9 NOT ASCERTAINED", -8: "-8 DK", -7: "-7 REFUSED", -1: "-1 INAPPLICABLE", 1: "UNINSURED", 2: "INSURED"}
INSCOV = {1: "ANY PRIVATE", 2: "PUBLIC ONLY", 3: "UNINSURED"}

PUF97 = load_meps("h97", columns=["UNINS05", "INSCOV05", "RXTOT05", "OBTOTV05", "ERTOT05", "VARPSU", "VARSTR", "PERWT05F"])
PUF97["UNINS05"] = apply_format(PUF97["UNINS05"], YESNO)
PUF97["INSCOV05"] = apply_format(PUF97["INSCOV05"], INSCOV)
PUF97["ONE"] = 1.0
design = dict(weight="PERWT05F", strata="VARSTR", psu="VARPSU")

# PROC SURVEYMEANS NOBS SUMWGT MEAN STDERR; DOMAIN UNINS05 INSCOV05
for var in ("RXTOT05", "OBTOTV05", "ERTOT05"):
    print(f"===== {var} =====")
    print(svy_mean(PUF97, var, **design).to_string(index=False), "\n")
    for dom in ("UNINS05", "INSCOV05"):
        res = svy_mean(PUF97, var, domain=dom, **design)
        sumwgt = svy_total(PUF97, "ONE", domain=dom, **design)[[dom, "estimate"]].rename(columns={"estimate": "sumwgt"})
        print(res.merge(sumwgt, on=dom).to_string(index=False), "\n")
