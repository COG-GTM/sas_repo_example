# ----------------------------------------------------------------
# PROGRAM:       C:\MEPS\PROG\EXAMPLE_E5.SAS
#
# DESCRIPTION:   THIS EXAMPLE SHOWS HOW TO COMPUTE EVENT-LEVEL
#                ESTIMATES.
#                IN THIS PROGRAM
#                (1) THE 2001 MEPS INPATIENT HOSPITAL STAYS FILE IS
#                    USED TO COMPUTE MEAN FACILITY EXPENSE PER
#                    STAY.
#                (2) THE 2001 MEPS OFFICE-BASED MEDICAL PROVIDER
#                    VISITS FILE IS USED TO COMPUTE MEAN
#                    EXPENSE PER OFFICE VISIT TO A MEDICAL
#                    PROVIDER.
#
# INPUT FILE:    (1) C:\MEPS\DATA\H59D.SAS7BDAT
#                         -- 2001 HOSPITAL STAYS FILE
#                (2) C:\MEPS\DATA\H59G.SAS7BDAT
#                         -- 2001 OFFICE-BASED VISITS FILE
#
# Python port of SAS/older_exercises_1996_to_2006/Estimation_examples/E5/E5.sas
# ----------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import add_cv_clm, design_df, load_meps, svy_mean  # noqa: E402

pd.set_option("display.width", 200)

design = dict(weight="PERWT01F", strata="VARSTR01", psu="VARPSU01")

# Inpatient hospital stays file: IPFXP01X = TOTAL HOSP. STAY EXPENSES
IP2001 = load_meps("h59d", columns=["DUPERSID", "IPFXP01X", "PERWT01F", "VARSTR01", "VARPSU01"])
print("MEAN FACILITY EXPENSE PER HOSPITAL STAY, 2001")
res = add_cv_clm(svy_mean(IP2001, "IPFXP01X", **design), design_df(IP2001, "VARSTR01", "VARPSU01"))
print(res.assign(SumWgt=IP2001["PERWT01F"].sum()).to_string(index=False), "\n")

# Office-based medical provider visits file: OBXP01X = TOTAL OFFICE VISIT EXPENSES
OB2001 = load_meps("h59g", columns=["DUPERSID", "OBXP01X", "PERWT01F", "VARSTR01", "VARPSU01"])
print("MEAN EXPENSE PER OFFICE-BASED MEDICAL PROVIDER VISIT, 2001")
res = add_cv_clm(svy_mean(OB2001, "OBXP01X", **design), design_df(OB2001, "VARSTR01", "VARPSU01"))
print(res.assign(SumWgt=OB2001["PERWT01F"].sum()).to_string(index=False))
