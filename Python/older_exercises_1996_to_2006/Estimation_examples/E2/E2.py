"""Python port of SAS/older_exercises_1996_to_2006/Estimation_examples/E2/E2.sas

/

PROGRAM:       <MEPS_DIR>/PROG/EXAMPLE_E2.SAS

DESCRIPTION:      THIS EXAMPLE SHOWS HOW TO
               (1) COMPUTE AVERAGE TOTAL HEALTHCARE EXPENDITURES
                   FOR CHILDREN 0-5 FOR THE 4 YEARS 1996-1999.
               (2) COMPUTE AVERAGE TOTAL EXPENDITURES FOR
                   1996-1997 AND 1998-1999 USING POOLED DATA.

EXPENDITURES:  FOR THE POOLED DATA FILES, EXPENDITURES ARE
               STANDARDIZED TO 1999 DOLLARS USING THE ANNUAL CPI
               FOR ALL URBAN CONSUMERS (CPI-U) FOR THE U.S. CITY
               AVERAGE FOR ALL ITEMS, 1982-84=100.  A TABLE WITH
               THIS DATA IS AVAILABLE AT THE CPI HOME PAGE:
               http://www.bls.gov/cpi/home.htm.

               ANNUAL CPI-U (1996) = 156.9
               ANNUAL CPI-U (1997) = 160.5
               ANNUAL CPI-U (1998) = 163.0
               ANNUAL CPI-U (1999) = 166.6

INPUT FILES:      (1) <MEPS_DIR>/DATA/H12.SD7 (1996 FULL-YEAR DATA FILE)
               (2) <MEPS_DIR>/DATA/H20.SD7 (1997 FULL-YEAR DATA FILE)
               (3) <MEPS_DIR>/DATA/H28.SD7 (1998 FULL-YEAR DATA FILE)
               (4) <MEPS_DIR>/DATA/H38.SD7 (1999 FULL-YEAR DATA FILE)
               (5) <MEPS_DIR>/DATA/H36.SD7
                                (1996-2002 POOLED ESTIMATION FILE)

/

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

COLS = ["N", "SumWgt", "Mean", "StdErr", "LowerCL", "UpperCL"]
AGEF = {slice(0, 5): "AGE 0-5"}  # OTHER = 'OTHER'

# 1996-1999 full-year files; 1996-1998 expenses inflated to 1999 dollars (CPI-U medical care)
H12 = read_meps("h12", ["DUPERSID", "TOTEXP96", "WTDPER96", "VARPSU96", "VARSTR96", "AGE96X"])
H12["TOTEXP"] = H12.TOTEXP96 * (166.6 / 156.9)
H20 = read_meps("h20", ["DUPERSID", "TOTEXP97", "WTDPER97", "VARPSU97", "VARSTR97", "AGE97X"])
H20["TOTEXP"] = H20.TOTEXP97 * (166.6 / 160.5)
H28 = read_meps("h28", ["DUPERSID", "TOTEXP98", "WTDPER98", "VARPSU98", "VARSTR98", "AGE98X"])
H28["TOTEXP"] = H28.TOTEXP98 * (166.6 / 163.0)
H38 = read_meps("h38", ["DUPERSID", "TOTEXP99", "PERWT99F", "VARPSU99", "VARSTR99", "AGE99X"])

# Single-year estimates by age (0-5 vs other) -------------------------------------
for df, yy, wt in [(H12, "96", "WTDPER96"), (H20, "97", "WTDPER97"), (H28, "98", "WTDPER98"), (H38, "99", "PERWT99F")]:
    df["AGE_F"] = sas_format(df[f"AGE{yy}X"], AGEF, other="OTHER")
    s, c = f"VARSTR{yy}", f"VARPSU{yy}"
    ddf = design_df(df, s, c, wt)
    show(f"19{yy}: TOTEXP{yy}", clm(surveymeans(df, f"TOTEXP{yy}", wt, s, c), ddf)[["VarName"] + COLS])
    show(f"19{yy}: TOTEXP{yy} by AGE{yy}X",
         clm(surveymeans(df, f"TOTEXP{yy}", wt, s, c, domain="AGE_F"), ddf)[["AGE_F", "VarName"] + COLS])

# Pooled variance file HC-036 (common strata/PSUs across years). The 1996-2002 release
# (STRA9602/PSU9602) used by the SAS program has been replaced on the MEPS website by
# the current release, so fall back to it (STRAyyyy/PSUyyyy) when H36 is not available.
try:
    H36 = read_meps("h36", ["DUPERSID", "STRA9602", "PSU9602"], download=False)
except FileNotFoundError:
    H36 = read_meps("h36u24")
    H36 = H36[["DUPERSID", "STRA9624", "PSU9624"]].rename(columns={"STRA9624": "STRA9602", "PSU9624": "PSU9602"})
    print("NOTE: HC-036 (1996-2002) not found; using pooled variance file h36u24 (STRA9624/PSU9624).")
H36 = H36.drop_duplicates("DUPERSID")


def pool(a, b, wa, wb, ya, yb, ta="TOTEXP", tb="TOTEXP"):
    """SET a b (RENAME=...); POOLWT/2; MERGE with H36 BY DUPERSID; IF A AND B."""
    p = pd.concat([
        a.rename(columns={wa: "POOLWT", f"AGE{ya}X": "AGE", ta: "TOTEXP"})[["DUPERSID", "TOTEXP", "AGE", "POOLWT"]],
        b.rename(columns={wb: "POOLWT", f"AGE{yb}X": "AGE", tb: "TOTEXP"})[["DUPERSID", "TOTEXP", "AGE", "POOLWT"]],
    ], ignore_index=True)
    p.loc[p.POOLWT > 0, "POOLWT"] /= 2
    p = p.merge(H36, on="DUPERSID", how="inner")
    p["AGE_F"] = sas_format(p.AGE, AGEF, other="OTHER")
    return p


for name, p in [("POOL9697", pool(H12, H20, "WTDPER96", "WTDPER97", "96", "97")),
                ("POOL9899", pool(H28, H38, "WTDPER98", "PERWT99F", "98", "99", tb="TOTEXP99"))]:
    ddf = design_df(p, "STRA9602", "PSU9602", "POOLWT")
    show(f"{name}: TOTEXP (1999 $)", clm(surveymeans(p, "TOTEXP", "POOLWT", "STRA9602", "PSU9602"), ddf)[["VarName"] + COLS])
    show(f"{name}: TOTEXP by AGE",
         clm(surveymeans(p, "TOTEXP", "POOLWT", "STRA9602", "PSU9602", domain="AGE_F"), ddf)[["AGE_F", "VarName"] + COLS])
