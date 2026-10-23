"""Python port of SAS/older_exercises_1996_to_2006/Estimation_examples/E3/E3.sas

/

PROGRAM:       <MEPS_DIR>/PROG/EXAMPLE_E3.SAS

DESCRIPTION:      THIS EXAMPLE SHOWS HOW TO CREATE A LONGITUDINAL
               FILE FOR 1999-2000 (PANEL 4) AND COMPUTE PERSON-
               LEVEL ESTIMATES FOR INSURANCE COVERAGE AND
               EXPENDITURES.

VARIABLES:     UNINS99, UNINS00
                  (INSURED ALL OF 1999, 2000)
               TOTEXP99, TOTEXP00
                  (TOTAL HEALTHCARE EXPENDITURES, 1999)

INPUT FILE:      (1) <MEPS_DIR>/DATA/H50.SAS7BDAT (2000 FULL-YEAR DATA FILE)
               (3) <MEPS_DIR>/DATA/H38.SAS7BDAT (1999 FULL-YEAR DATA FILE)
               (5) <MEPS_DIR>/DATA/H58.SAS7BDAT
                                (PANEL 4 LONGITUDINAL WEIGHT FILE)

/

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# Panel 4 persons from the 1999 (HC-038) and 2000 (HC-050) full-year files ----------
H38 = read_meps("h38", ["DUPERSID", "UNINS99", "TOTEXP99", "PANEL99"])
H38 = H38[H38.PANEL99 == 4].drop(columns="PANEL99")
H50 = read_meps("h50", ["DUPERSID", "UNINS00", "TOTEXP00", "PANEL00"])
H50 = H50[H50.PANEL00 == 4].drop(columns="PANEL00")
# Panel 4 longitudinal file (HC-058), persons in the population both years
# (the re-released HC-058 on the MEPS website names these LONGWT and YEARIND)
H58 = read_meps("h58").rename(columns={"LONGWT": "LONGWTP4", "YEARIND": "YRINDP4"})
H58 = H58.loc[H58.YRINDP4 == 1, ["DUPERSID", "LONGWTP4", "VARPSUP4", "VARSTRP4"]]

LONGP4 = H38.merge(H50, on="DUPERSID", how="inner").merge(H58, on="DUPERSID", how="inner")  # IF A AND B AND C
LONGP4["UNINS99"] = np.where(LONGP4.UNINS99 == 1, 1, 0)
LONGP4["UNINS00"] = np.where(LONGP4.UNINS00 == 1, 100, 0)
LONGP4["TOTEXP99"] = np.where(LONGP4.TOTEXP99 > 0, 1, 0)
LONGP4["TOTEXP00"] = np.where(LONGP4.TOTEXP00 > 0, 100, 0)

# PROC FREQ ... WEIGHT LONGWTP4 (weighted point estimates only)
for v99 in [1, 0]:
    sub = LONGP4[LONGP4.TOTEXP99 == v99]
    f = sub.groupby("TOTEXP00").LONGWTP4.sum().rename("Frequency").to_frame()
    f["Percent"] = 100 * f.Frequency / f.Frequency.sum()
    f["CumFrequency"], f["CumPercent"] = f.Frequency.cumsum(), f.Percent.cumsum()
    show(f"Weighted PROC FREQ of TOTEXP00 WHERE TOTEXP99 = {v99}", f.reset_index())

design = dict(weight="LONGWTP4", strata="VARSTRP4", cluster="VARPSUP4")
ddf = design_df(LONGP4, "VARSTRP4", "VARPSUP4", "LONGWTP4")
COLS = ["VarName", "N", "SumWgt", "Mean", "StdErr", "LowerCL", "UpperCL"]
for v, dom in [("UNINS00", "UNINS99"), ("TOTEXP00", "TOTEXP99")]:
    show(f"{v}", clm(surveymeans(LONGP4, v, **design), ddf)[COLS])
    show(f"{v} by {dom}", clm(surveymeans(LONGP4, v, domain=dom, **design), ddf)[[dom] + COLS])
