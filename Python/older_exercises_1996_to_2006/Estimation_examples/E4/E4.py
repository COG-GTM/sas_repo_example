# ----------------------------------------------------------------
# PROGRAM:       C:\MEPS\PROG\EXAMPLE_E4.SAS
#
# DESCRIPTION:   THIS EXAMPLE SHOWS HOW TO COMPUTE FAMILY-LEVEL
#                ESTIMATES, USING THE MEPS DEFINITION OF FAMILY
#                RATHER THAN THE CPS DEFINITION.
#                SEE SECTION 3.3 OF THE DOCUMENTATION FOR HC-060
#                (THE 2001 MEPS FILL-YEAR FILE).
#
#                THIS PROGRAM GENERATES THE FOLLOWING FAMILY-LEVEL
#                ESTIMATES:
#                (1) MEAN NUMBER OF PERSONS PER FAMILY.
#                (2) 2001 MEAN TOTAL HEALTHCARE EXPENSES PER
#                    FAMILY.
#                (3) 2001 MEAN TOTAL HEALTHCARE EXPENSES PER
#                    FAMILY SIZE.
#
# INPUT FILE:    (1) C:\MEPS\DATA\H60.SAS7BDAT
#                         -- 2001 MEPS FULL-YEAR FILE
#
# Python port of SAS/older_exercises_1996_to_2006/Estimation_examples/E4/E4.sas
# ----------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import add_cv_clm, apply_format, crosstab_list, design_df, load_meps, svy_mean  # noqa: E402

pd.set_option("display.width", 200)

H60 = load_meps("h60", columns=["DUID", "FAMIDYR", "DUPERSID", "FAMWT01F", "VARSTR01", "VARPSU01", "FAMRFPYR",
                                "FAMSZEYR", "TOTEXP01"])  # fmt: skip
# DUIDFAMY = PUT(DUID, Z5.) || TRIM(FAMIDYR)
H60["DUIDFAMY"] = H60["DUID"].astype(int).map("{:05d}".format) + H60["FAMIDYR"].astype(str).str.strip()
H60 = H60.sort_values("DUIDFAMY", kind="stable")

# Sum total expenses to the family level (FAMTOT01 = TOTAL EXPENSES (FAMILY));
# keep one record per family (LAST.DUIDFAMY) with a positive family weight.
H60_CHK = H60.assign(FAMTOT01=H60.groupby("DUIDFAMY")["TOTEXP01"].cumsum())
last = H60_CHK.drop_duplicates("DUIDFAMY", keep="last")
FAM_H60 = last[last["FAMWT01F"] > 0].drop(columns="TOTEXP01")

print(crosstab_list(FAM_H60, ["FAMSZEYR"]).to_string(index=False), "\n")
ids = ["40001A", "40006A", "40007A", "40010A", "40011A"]
print(H60_CHK.loc[H60_CHK["DUIDFAMY"].isin(ids), ["DUIDFAMY", "DUPERSID", "TOTEXP01", "FAMTOT01"]].to_string(index=False))
print(FAM_H60.loc[FAM_H60["DUIDFAMY"].isin(ids), ["DUIDFAMY", "FAMTOT01"]].to_string(index=False), "\n")

design = dict(weight="FAMWT01F", strata="VARSTR01", psu="VARPSU01")
dof = design_df(FAM_H60, "VARSTR01", "VARPSU01")
print("FAMILY-LEVEL MEANS: PERSONS PER FAMILY, TOTAL EXPENSES PER FAMILY")
print(add_cv_clm(svy_mean(FAM_H60, ["FAMSZEYR", "FAMTOT01"], **design), dof).to_string(index=False), "\n")

FAMF = [(1, 1, "1"), (2, 2, "2"), (3, 3, "3"), (4, 4, "4"), (5, np.inf, "5+")]
FAM_H60["FAMSIZE"] = apply_format(FAM_H60["FAMSZEYR"], FAMF)
print("MEAN TOTAL EXPENSES PER FAMILY, BY FAMILY SIZE")
print(add_cv_clm(svy_mean(FAM_H60, "FAMTOT01", domain="FAMSIZE", **design), dof).to_string(index=False))
