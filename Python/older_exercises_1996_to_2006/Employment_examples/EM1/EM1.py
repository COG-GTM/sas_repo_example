# ----------------------------------------------------------------
# PROGRAM:      C:\MEPS\PROG\EXAMPLE_EM1.SAS
#
# DESCRIPTION:  THIS EXAMPLE SHOWS HOW TO BUILD AN ANALYTIC FILE
#               AND CREATE NEW VARIABLES TO EXAMINE THE
#               RELATIONSHIP BETWEEN PERCEIVED HEALTH STATUS
#               AND A PERSON'S WEEKLY EARNINGS, WHERE WEEKLY
#               EARNINGS ARE THOSE OF THE CURRENT MAIN JOB.
#               PERSON-LEVEL RECORDS ARE DIVIDED INTO QUARTILES
#               BASED ON WEEKLY EARNINGS AND PERSON-LEVEL WEIGHTS.
#               THE RESULT IS 4 EQUALLY WEIGHTED QUARTILES FOR
#               WEEKLY EARNINGS.
#
# INPUT FILE:   C:\MEPS\DATA\H62.SAS7BDAT (2002 FULL-YEAR POPULATION)
#                    -- PERSON-LEVEL FILE
#
# Python port of SAS/older_exercises_1996_to_2006/Employment_examples/EM1/EM1.sas
# ----------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import add_domain, apply_format, crosstab_list, load_meps, svy_mean  # noqa: E402

pd.set_option("display.width", 200)

HLTHF = {1: "1 EXCELLENT", 2: "2 VERY GOOD", 3: "3 GOOD", 4: "4 FAIR", 5: "5 POOR"}
QUARTF = {1: "1 LOWEST", 2: "2", 3: "3", 4: "4 HIGHEST"}
WKLYF = [(0.13, 49.99 - 1e-9, "    0.13 -  49.99"), (50, 499.99, "   50.00 - 499.99"), (500, 999.99, "  500.00 - 999.99"),
         (1000, np.inf, "1,000.00+        ")]  # fmt: skip

cols = ["DUPERSID", "PERWT02P", "VARSTR", "VARPSU", "EMPST42", "SELFCM42", "SELFCM31", "HRWG42X", "HRWG31X", "HOUR42",
        "HOUR31", "RTHLTH42", "TEMPJB42", "AGE42X"]  # fmt: skip
# HC-062 (2002 population characteristics, weight PERWT02P) is no longer on
# the MEPS website; fall back to the 2002 consolidated file HC-070 (PERWT02F).
try:
    H62 = load_meps("h62", columns=cols)
except FileNotFoundError:
    H62 = load_meps("h70", columns=[c.replace("PERWT02P", "PERWT02F") for c in cols])
    H62 = H62.rename(columns={"PERWT02F": "PERWT02P"})

# Persons with a current main job (CMJ) in rounds 4/2 that reported earnings,
# weekly hours and perceived health status. A value of -2 for the '42'
# variables means the value comes from the prior round ('31' variables).
s = H62
sel = (
    s["EMPST42"].isin([1, 2])
    & ((s["SELFCM42"] == 2) | ((s["SELFCM42"] == -2) & (s["SELFCM31"] == 2)))
    & ((s["HRWG42X"] == -10) | (s["HRWG42X"] > 0) | ((s["HRWG42X"] == -2) & ((s["HRWG31X"] == -10) | (s["HRWG31X"] > 0))))
    & ((s["HOUR42"] > 0) | ((s["HOUR42"] == -2) & (s["HOUR31"] > 0)))
    & s["RTHLTH42"].isin([1, 2, 3, 4, 5])
    & (s["TEMPJB42"] == 2)
    & (s["AGE42X"] > 24)
)
H62["IN_SUBSET"] = sel.astype(int)
hrly = np.where(H62["HRWG42X"] == -2, H62["HRWG31X"], H62["HRWG42X"])
H62["HRLYWAGE"] = np.where(hrly == -10, 61.98, hrly)  # top-coded wage
H62["HOURS"] = np.where(H62["HOUR42"] > 0, H62["HOUR42"], np.where(H62["HOUR42"] == -2, H62["HOUR31"], np.nan))
H62["WKLYEARN"] = H62["HRLYWAGE"] * H62["HOURS"]  # WEEKLY EARNINGS

# Four groups of equal weight (WEEKLY EARNINGS QUARTILE) using cumulative weights
h62a = H62[sel].sort_values("WKLYEARN", kind="stable").copy()
tot_wt = h62a["PERWT02P"].sum()
cum = h62a["PERWT02P"].cumsum()
h62a["QUARTILE"] = np.select([cum <= tot_wt * 0.25, cum <= tot_wt * 0.5, cum <= tot_wt * 0.75], [1, 2, 3], default=4)
H62 = H62.merge(h62a[["DUPERSID", "QUARTILE"]], on="DUPERSID", how="left")
h62b = H62[H62["IN_SUBSET"] == 1].copy()
h62b["WKLY_F"] = apply_format(h62b["WKLYEARN"], WKLYF)

for var in ("QUARTILE", "WKLY_F"):  # weighted PROC FREQ
    t = h62b.groupby(var)["PERWT02P"].sum().rename("FREQUENCY").to_frame()
    t["PERCENT"] = 100 * t["FREQUENCY"] / t["FREQUENCY"].sum()
    print(t, "\n")
print(crosstab_list(h62b, ["WKLY_F"]).to_string(index=False), "\n")
print(crosstab_list(h62b.assign(RTHLTH42=apply_format(h62b["RTHLTH42"], HLTHF)), ["QUARTILE", "RTHLTH42"]).to_string(index=False))

# PROC TABULATE: weighted row percent of health status within quartile
tab = pd.crosstab(h62b["QUARTILE"].map(QUARTF), h62b["RTHLTH42"].map(HLTHF), values=h62b["PERWT02P"], aggfunc="sum",
                  normalize="index") * 100  # fmt: skip
print(tab.round(2), "\n")

# PROC SURVEYMEANS; DOMAIN QUARTILE. SAS runs this on the subset file; here
# the full person file is kept and the subset is part of the domain so the
# standard errors reflect the complete MEPS design.
H62 = add_domain(H62, H62["IN_SUBSET"] == 1, "PERWT02P", name="SUBSET")
H62["QUARTILE"] = apply_format(H62["QUARTILE"].fillna(0), QUARTF | {0: "NOT IN SUBSET"})
res = svy_mean(H62, "RTHLTH42", domain="QUARTILE", weight="PERWT02P", strata="VARSTR", psu="VARPSU")
print(res[res["QUARTILE"] != "NOT IN SUBSET"].to_string(index=False))
