# -----------------------------------------------------------------------------
# DESCRIPTION:  THIS PROGRAM ILLUSTRATES HOW TO POOL MEPS DATA FILES FROM
#               DIFFERENT YEARS. THE EXAMPLE USED IS POPULATION AGE 26-30 WHO
#               ARE UNINSURED BUT HAVE HIGH INCOME. DATA FROM 2015 AND 2016 ARE
#               POOLED.
#
#               VARIABLES WITH YEAR-SPECIFIC NAMES MUST BE RENAMED BEFORE
#               COMBINING FILES. IN THIS PROGRAM THE INSURANCE COVERAGE
#               VARIABLES 'INSCOV15' AND 'INSCOV16' ARE RENAMED TO 'INSCOV'.
#
#               SEE HC-036 (1996-2015 POOLED ESTIMATION FILE) FOR INSTRUCTIONS
#               ON POOOLING AND CONSIDERATIONS FOR VARIANCE ESTIMATION FOR
#               PRE-2002 DATA.
#
# INPUT FILE:   (1) C:\MEPS\SAS\DATA\H192.SAS7BDAT (2016 FULL-YEAR FILE)
#               (2) C:\MEPS\SAS\DATA\H181.SAS7BDAT (2015 FULL-YEAR FILE)
#
# Python port of SAS/workshop_exercises/exercise_4a/Exercise4a.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, crosstab_list, load_meps, svy_mean  # noqa: E402

pd.set_option("display.width", 200)

print("2018 AHRQ MEPS DATA USERS WORKSHOP")
print("EXERCISE6: POOL MEPS DATA FILES FROM DIFFERENT YEARS (2015 and 2016)\n")

# Formats (PROC FORMAT) -------------------------------------------------------

POVCAT = {1: "1 POOR/NEGATIVE", 2: "2 NEAR POOR", 3: "3 LOW INCOME", 4: "4 MIDDLE INCOME", 5: "5 HIGH INCOME"}
INSF = {1: "1 ANY PRIVATE", 2: "2 PUBLIC ONLY", 3: "3 UNINSURED"}
AGE = [(26, 30, "26-30"), (0, 25, "0-25"), (31, np.inf, "31+")]


def read_year(name, yy):
    cols = ["DUPERSID", f"INSCOV{yy}", f"PERWT{yy}F", "VARSTR", "VARPSU", f"POVCAT{yy}", "AGELAST", f"TOTSLF{yy}"]
    df = load_meps(name, columns=cols)
    df = df[df[f"PERWT{yy}F"] > 0]
    print(f"UNWEIGHTED FREQUENCY FOR 20{yy} FY PERSONS WITH AGE 26-30")
    sub = df[df["AGELAST"].between(26, 30)]
    sub = sub.assign(**{f"INSCOV{yy}": apply_format(sub[f"INSCOV{yy}"], INSF),
                        f"POVCAT{yy}": apply_format(sub[f"POVCAT{yy}"], POVCAT)})  # fmt: skip
    print(crosstab_list(sub, [f"POVCAT{yy}", f"INSCOV{yy}"]), "\n")
    # RENAME YEAR SPECIFIC VARIABLES PRIOR TO COMBINING FILES
    return df.rename(
        columns={f"INSCOV{yy}": "INSCOV", f"PERWT{yy}F": "PERWT", f"POVCAT{yy}": "POVCAT", f"TOTSLF{yy}": "TOTSLF"}
    )


YR1X = read_year("h181", "15")  # FREQUENCY OF 2015
YR2X = read_year("h192", "16")  # FREQUENCY OF 2016

POOL = pd.concat([YR1X, YR2X], ignore_index=True)
POOL["POOLWT"] = POOL["PERWT"] / 2
# SUBPOP = 1: POPULATION WITH AGE=26-30, UNINSURED WHOLE YEAR, AND HIGH INCOME
POOL["SUBPOP"] = np.where(POOL["AGELAST"].between(26, 30) & (POOL["POVCAT"] == 5) & (POOL["INSCOV"] == 3), 1, 2)

print("CHECK MISSING VALUES ON THE COMBINED DATA")
print(pd.DataFrame({"N": POOL.notna().sum(), "NMISS": POOL.isna().sum()}), "\n")

print("SUPPORTING CROSSTAB FOR THE CREATION OF THE SUBPOP FLAG")
print(crosstab_list(POOL, ["SUBPOP"]))
print(crosstab_list(POOL.assign(AGELAST=apply_format(POOL["AGELAST"], AGE)), ["SUBPOP", "AGELAST", "POVCAT", "INSCOV"]))

# WEIGHTED ESTIMATE ON TOTSLF FOR COMBINED DATA (DOMAIN SUBPOP("1")) ----------
print("\nWEIGHTED ESTIMATE ON TOTSLF FOR COMBINED DATA W/AGE=26-30, UNINSURED WHOLE YEAR, AND HIGH INCOME")
res = svy_mean(POOL, "TOTSLF", weight="POOLWT", strata="VARSTR", psu="VARPSU", domain="SUBPOP")
print(res[res["SUBPOP"] == 1].to_string(index=False))
