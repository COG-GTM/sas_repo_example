"""Python port of SAS/workshop_exercises/exercise_4b/Exercise4b.sas

DESCRIPTION:  THIS PROGRAM ILLUSTRATES HOW TO POOL MEPS LONGITUDINAL DATA FILES FROM DIFFERENT PANELS
              THE EXAMPLE USED IS PANELS 17-19 POPULATION AGE 26-30 WHO ARE UNINSURED BUT HAVE HIGH INCOME IN THE FIRST YEAR

                DATA FROM PANELS 17, 18, AND 19 ARE POOLED.

INPUT FILE:     (1) <MEPS_DIR>/SAS/DATA/H183.SAS7BDAT (PANEL 19 LONGITUDINAL FILE)
                (2) <MEPS_DIR>/SAS/DATA/H172.SAS7BDAT (PANEL 18 LONGITUDINAL FILE)
                (3) <MEPS_DIR>/SAS/DATA/H164.SAS7BDAT (PANEL 17 LONGITUDINAL FILE)

;
IMPORTANT NOTES: Use the next 6 lines of code, if you want to specify an alternative destination for SAS log and
SAS procedure output.

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# PROC FORMAT
POVCAT = {1: "1 POOR/NEGATIVE", 2: "2 NEAR POOR", 3: "3 LOW INCOME", 4: "4 MIDDLE INCOME", 5: "5 HIGH INCOME"}
INSF = {-1: "-1 INAPPLICABLE", 1: "1 ANY PRIVATE", 2: "2 PUBLIC ONLY", 3: "3 UNINSURED"}
AGE = {-1: "-1 INAPPLICABLE", slice(26, 30): "26-30", slice(0, 25): "0-25, 31+", slice(31, None): "0-25, 31+"}

# Pool panels 17, 18 and 19 longitudinal files (SAS: SET H164 H172 H183) ---------------
keep = ["DUPERSID", "INSCOVY1", "INSCOVY2", "LONGWT", "VARSTR", "VARPSU", "POVCATY1", "AGEY1X", "PANEL"]
POOL = pd.concat([read_meps(f, keep) for f in ("h164", "h172", "h183")], ignore_index=True)
POOL["POOLWT"] = POOL.LONGWT / 3
POOL["SUBPOP"] = np.where((POOL.INSCOVY1 == 3) & POOL.AGEY1X.between(26, 30) & (POOL.POVCATY1 == 5), 1, 2)

print(POOL.notna().sum().rename("N").to_frame().assign(NMISS=POOL.isna().sum()))
print(freq_list(POOL, ["SUBPOP"]))
print(freq_list(POOL, ["SUBPOP", "PANEL"]))
print(freq_list(POOL.assign(AGEY1X=sas_format(POOL.AGEY1X, AGE), POVCATY1=sas_format(POOL.POVCATY1, POVCAT),
                            INSCOVY1=sas_format(POOL.INSCOVY1, INSF)), ["SUBPOP", "INSCOVY1", "AGEY1X", "POVCATY1"]))

# Year-2 insurance status of persons uninsured in year 1 (CLASS INSCOVY2, DOMAIN SUBPOP('1'))
POOL["INSCOVY2"] = sas_format(POOL.INSCOVY2, INSF)
out = surveyfreq(POOL, ["SUBPOP", "INSCOVY2"], weight="POOLWT", strata="VARSTR", cluster="VARPSU")
out["Mean"], out["StdErr"] = out.RowPercent / 100, out.RowStdErr / 100
show("Health insurance coverage in year 2, SUBPOP = 1 (pooled panels 17-19)",
     out[out.SUBPOP == 1][["INSCOVY2", "Frequency", "Mean", "StdErr"]])
