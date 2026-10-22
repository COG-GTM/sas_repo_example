# ----------------------------------------------------------------------
# PROGRAM:      C:\MEPS\PROG\EXAMPLE10.SAS
#
# DESCRIPTION:  THIS EXAMPLE COMPARES HOSPITAL INPATIENT EXPENDITURES
#               (FACILITY, PHYSICIAN, TOTAL) FOR STAYS THAT DO AND DO
#               NOT INCLUDE FACILITY EXPENDITURES FOR THE PRECEDING
#               EMERGENCY ROOM VISIT.
#               RECORDS WITH PRECEDING ER FACILITY EXPENDITURES ARE
#               IDENTIFIED BY THE VARIABLE ERHEVIDX.
#
# INPUT FILE:   C:\MEPS\DATA\H77F.SAS7BDAT (2003 HOSP. INPATIENT FILE)
#               (the SAS code reads H77D, the 2003 hospital inpatient stays
#               file, and H67D, the 2002 hospital inpatient stays file)
#
# Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M10/M10.sas
# ----------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import load_meps, svy_glm, svy_mean, svy_total  # noqa: E402

pd.set_option("display.width", 200)

EREXPF = {0: "ER_YES", 1: "ER__NO"}

STAZ2002 = load_meps("h67d", columns=["DUPERSID", "ERHEVIDX", "IPXP02X", "IPFXP02X", "IPDXP02X", "PERWT02F", "VARSTR",
                                      "VARPSU"])  # fmt: skip
STAZ2003 = load_meps("h77d", columns=["DUPERSID", "ERHEVIDX", "IPXP03X", "IPFXP03X", "IPDXP03X", "PERWT03F", "VARSTR",
                                      "VARPSU"])  # fmt: skip
# ER_FACEX = 0 if the stay includes ER facility expenditures
erhevidx = STAZ2003["ERHEVIDX"].astype(str).str.strip()
STAZ2003["ER_FACEX"] = np.where(~erhevidx.isin(["-1", "-1.0"]), 0, 1)
STAZ2003["ER_FACEX_F"] = STAZ2003["ER_FACEX"].map(EREXPF)
STAZ2003["ONE"] = 1.0


def means(df, var, weight, domain=None):
    """PROC SURVEYMEANS NOBS SUMWGT MEAN STDERR [DOMAIN]."""
    d = dict(weight=weight, strata="VARSTR", psu="VARPSU", domain=domain)
    res = svy_mean(df, var, **d)
    df = df.assign(ONE=1.0)
    sw = svy_total(df, "ONE", **d).rename(columns={"estimate": "sumwgt"})
    keys = [domain] if domain else []
    print(res.merge(sw[keys + ["sumwgt"]], on=keys) if keys else res.assign(sumwgt=sw["sumwgt"].iloc[0]))
    print()


means(STAZ2002, "IPXP02X", "PERWT02F")
means(STAZ2003, "IPXP03X", "PERWT03F")
for var in ("IPXP03X", "IPFXP03X", "IPDXP03X"):
    means(STAZ2003, var, "PERWT03F", domain="ER_FACEX_F")
    # PROC SURVEYREG; MODEL var = ER_FACEX
    print(f"SURVEYREG: {var} = ER_FACEX")
    print(svy_glm(f"{var} ~ ER_FACEX", STAZ2003, weight="PERWT03F").summary_frame().to_string(), "\n")
