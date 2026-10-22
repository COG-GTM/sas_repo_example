# ----------------------------------------------------------------
# PROGRAM:       C:\MEPS\PROG\EXAMPLE_E2.SAS
#
# DESCRIPTION:   THIS EXAMPLE SHOWS HOW TO
#                (1) COMPUTE AVERAGE TOTAL HEALTHCARE EXPENDITURES
#                    FOR CHILDREN 0-5 FOR THE 4 YEARS 1996-1999.
#                (2) COMPUTE AVERAGE TOTAL EXPENDITURES FOR
#                    1996-1997 AND 1998-1999 USING POOLED DATA.
#
# EXPENDITURES:  FOR THE POOLED DATA FILES, EXPENDITURES ARE
#                STANDARDIZED TO 1999 DOLLARS USING THE ANNUAL CPI
#                FOR ALL URBAN CONSUMERS (CPI-U) FOR THE U.S. CITY
#                AVERAGE FOR ALL ITEMS, 1982-84=100.  A TABLE WITH
#                THIS DATA IS AVAILABLE AT THE CPI HOME PAGE:
#                http://www.bls.gov/cpi/home.htm.
#
#                ANNUAL CPI-U (1996) = 156.9
#                ANNUAL CPI-U (1997) = 160.5
#                ANNUAL CPI-U (1998) = 163.0
#                ANNUAL CPI-U (1999) = 166.6
#
# INPUT FILES:   (1) C:\MEPS\DATA\H12.SD7 (1996 FULL-YEAR DATA FILE)
#                (2) C:\MEPS\DATA\H20.SD7 (1997 FULL-YEAR DATA FILE)
#                (3) C:\MEPS\DATA\H28.SD7 (1998 FULL-YEAR DATA FILE)
#                (4) C:\MEPS\DATA\H38.SD7 (1999 FULL-YEAR DATA FILE)
#                (5) C:\MEPS\DATA\H36.SD7
#                                 (1996-2002 POOLED ESTIMATION FILE)
#
# Python port of SAS/older_exercises_1996_to_2006/Estimation_examples/E2/E2.sas
# ----------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import add_cv_clm, apply_format, design_df, load_meps, svy_mean  # noqa: E402

pd.set_option("display.width", 200)

AGEF = [(0, 5, "AGE 0-5")]  # OTHER = 'OTHER'
CPI = {96: 156.9, 97: 160.5, 98: 163.0, 99: 166.6}

# Read the required variables from the full-year files. TOTEXP is standardized
# to 1999 dollars by multiplying TOTEXPyy by (CPI_1999 / CPI_yy) for pooling.
files = {96: ("h12", "WTDPER96"), 97: ("h20", "WTDPER97"), 98: ("h28", "WTDPER98"), 99: ("h38", "PERWT99F")}
data = {}
for yy, (name, wt) in files.items():
    df = load_meps(name, columns=["DUPERSID", f"TOTEXP{yy}", wt, f"VARPSU{yy}", f"VARSTR{yy}", f"AGE{yy}X"])
    df["TOTEXP"] = df[f"TOTEXP{yy}"] * (CPI[99] / CPI[yy])
    df["AGEF"] = apply_format(df[f"AGE{yy}X"], AGEF, default="OTHER")
    data[yy] = df

# (1) Average total expenditures for children 0-5, each year 1996-1999 ---------
for yy, (name, wt) in files.items():
    df, st, psu = data[yy], f"VARSTR{yy}", f"VARPSU{yy}"
    res = svy_mean(df, f"TOTEXP{yy}", weight=wt, domain="AGEF", strata=st, psu=psu)
    print(f"19{yy}: TOTEXP{yy} by age (DOMAIN AGE{yy}X)")
    print(add_cv_clm(res, design_df(df, st, psu)).to_string(index=False), "\n")

# (2) Pooled estimates: POOLWT = weight / 2, with STRA9602 / PSU9602 from HC-036
H36 = load_meps("h36", columns=["DUPERSID", "STRA9602", "PSU9602"])


def pool(y1, y2):
    frames = [data[y].rename(columns={files[y][1]: "POOLWT"})[["DUPERSID", "TOTEXP", "AGEF", "POOLWT"]] for y in (y1, y2)]
    p = pd.concat(frames, ignore_index=True)
    p.loc[p["POOLWT"] > 0, "POOLWT"] /= 2
    return p.merge(H36, on="DUPERSID", how="inner")


for y1, y2 in ((96, 97), (98, 99)):
    p = pool(y1, y2)
    res = svy_mean(p, "TOTEXP", weight="POOLWT", domain="AGEF", strata="STRA9602", psu="PSU9602")
    print(f"POOLED 19{y1}-19{y2} (1999 DOLLARS): TOTEXP by age")
    print(add_cv_clm(res, design_df(p, "STRA9602", "PSU9602")).to_string(index=False), "\n")
