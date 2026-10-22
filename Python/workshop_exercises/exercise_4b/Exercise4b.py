# -----------------------------------------------------------------------------
# DESCRIPTION:  THIS PROGRAM ILLUSTRATES HOW TO POOL MEPS LONGITUDINAL DATA
#               FILES FROM DIFFERENT PANELS. THE EXAMPLE USED IS PANELS 17-19
#               POPULATION AGE 26-30 WHO ARE UNINSURED BUT HAVE HIGH INCOME IN
#               THE FIRST YEAR. DATA FROM PANELS 17, 18, AND 19 ARE POOLED.
#
# INPUT FILE:   (1) C:\MEPS\SAS\DATA\H183.SAS7BDAT (PANEL 19 LONGITUDINAL FILE)
#               (2) C:\MEPS\SAS\DATA\H172.SAS7BDAT (PANEL 18 LONGITUDINAL FILE)
#               (3) C:\MEPS\SAS\DATA\H164.SAS7BDAT (PANEL 17 LONGITUDINAL FILE)
#
# Python port of SAS/workshop_exercises/exercise_4b/Exercise4b.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, crosstab_list, load_meps, svy_freq  # noqa: E402

pd.set_option("display.width", 200)

print("2018 AHRQ MEPS DATA USERS WORKSHOP")
print("EXERCISE8: POOL MEPS DATA FILES FROM DIFFERENT PANELS (PANELS 17, 18, 19)\n")

# Formats (PROC FORMAT) -------------------------------------------------------

POVCAT = {1: "1 POOR/NEGATIVE", 2: "2 NEAR POOR", 3: "3 LOW INCOME", 4: "4 MIDDLE INCOME", 5: "5 HIGH INCOME"}
INSF = {-1: "-1 INAPPLICABLE", 1: "1 ANY PRIVATE", 2: "2 PUBLIC ONLY", 3: "3 UNINSURED"}
AGE = [(-1, -1, "-1 INAPPLICABLE"), (26, 30, "26-30"), (0, 25, "0-25, 31+"), (31, np.inf, "0-25, 31+")]

# POOL THE LONGITUDINAL FILES (variables are not year-specific) ----------------

keep = ["DUPERSID", "INSCOVY1", "INSCOVY2", "LONGWT", "VARSTR", "VARPSU", "POVCATY1", "AGEY1X", "PANEL"]
POOL = pd.concat([load_meps(n, columns=keep) for n in ("h164", "h172", "h183")], ignore_index=True)
POOL["POOLWT"] = POOL["LONGWT"] / 3
# SUBPOP = 1: POPULATION WITH AGE=26-30, UNINSURED, AND HIGH INCOME IN FIRST YEAR
POOL["SUBPOP"] = np.where(
    (POOL["INSCOVY1"] == 3) & POOL["AGEY1X"].between(26, 30) & (POOL["POVCATY1"] == 5), 1, 2
)

print("CHECK MISSING VALUES ON THE COMBINED DATA")
print(pd.DataFrame({"N": POOL.notna().sum(), "NMISS": POOL.isna().sum()}), "\n")

print("SUPPORTING CROSSTAB FOR THE CREATION OF THE SUBPOP FLAG")
qc = POOL.assign(
    AGEY1X=apply_format(POOL["AGEY1X"], AGE),
    POVCATY1=apply_format(POOL["POVCATY1"], POVCAT),
    INSCOVY1=apply_format(POOL["INSCOVY1"], INSF),
)
print(crosstab_list(qc, ["SUBPOP"]))
print(crosstab_list(qc, ["SUBPOP", "PANEL"]))
print(crosstab_list(qc, ["SUBPOP", "INSCOVY1", "AGEY1X", "POVCATY1"]))

# INSURANCE STATUS IN THE SECOND YEAR (CLASS INSCOVY2; DOMAIN SUBPOP('1')) ------
print(
    "\nINSURANCE STATUS IN THE SECOND YEAR FOR THOSE W/ AGE=26-30, UNINSURED WHOLE YEAR, "
    "AND HIGH INCOME IN THE FIRST YEAR"
)
POOL["INSCOVY2"] = apply_format(POOL["INSCOVY2"], INSF)
res = svy_freq(POOL, "INSCOVY2", weight="POOLWT", strata="VARSTR", psu="VARPSU", domain="SUBPOP")
print(res[res["SUBPOP"] == 1].to_string(index=False))
