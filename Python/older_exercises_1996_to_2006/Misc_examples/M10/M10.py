"""Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M10/M10.sas

/

PROGRAM:       <MEPS_DIR>/PROG/EXAMPLE10.SAS

DESCRIPTION:      THIS EXAMPLE COMPARES HOSPITAL INPATIENT EXPENDITURES
               (FACILITY, PHYSICIAN, TOTAL) FOR STAYS THAT DO AND DO
               NOT INCLUDE FACILITY EXPENDITURES FOR THE PRECEDING
               EMERGENCY ROOM VISIT.

                 RECORDS WITH PRECEDING ER FACILITY EXPENDITURES ARE
                 IDENTIFIED BY THE VARIABLE ERHEVIDX.

INPUT FILE:      <MEPS_DIR>/DATA/H77F.SAS7BDAT (2003 HOSP. INPATIENT FILE)

/

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

EREXPF = {0: "ER_YES", 1: "ER__NO"}

# 2002 (HC-067D) and 2003 (HC-077D) hospital inpatient stays files
staz2002 = read_meps("h67d", ["DUPERSID", "ERHEVIDX", "IPXP02X", "IPFXP02X", "IPDXP02X", "PERWT02F", "VARSTR",
                              "VARPSU"])
staz2003 = read_meps("h77d", ["DUPERSID", "ERHEVIDX", "IPXP03X", "IPFXP03X", "IPDXP03X", "PERWT03F", "VARSTR",
                              "VARPSU"])
# ER_FACEX = 0 if the stay includes ER facility expenses (linked ER event), else 1
staz2003["ER_FACEX"] = np.where(staz2003.ERHEVIDX.astype(str).str.strip() != "-1", 0, 1)
staz2003["ER_FACEX_F"] = sas_format(staz2003.ER_FACEX, EREXPF)

show("2002: IPXP02X", surveymeans(staz2002, "IPXP02X", "PERWT02F"))
for v in ["IPXP03X", "IPFXP03X", "IPDXP03X"]:
    show(f"2003: {v}", surveymeans(staz2003, v, "PERWT03F"))
    show(f"2003: {v} by ER_FACEX", surveymeans(staz2003, v, "PERWT03F", domain="ER_FACEX_F"))
    show(f"PROC SURVEYREG: {v} = ER_FACEX", surveyreg(staz2003, f"{v} ~ ER_FACEX", "PERWT03F", family="linear"))
