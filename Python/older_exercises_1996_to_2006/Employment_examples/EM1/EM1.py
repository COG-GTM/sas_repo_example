# -----------------------------------------------------------------------------
# Python port of SAS/older_exercises_1996_to_2006/Employment_examples/EM1/EM1.sas
#
# DESCRIPTION: PERCEIVED HEALTH STATUS AND WEEKLY EARNINGS, 2002. FOR CURRENTLY EMPLOYED,
#              NON-SELF-EMPLOYED, NON-TEMPORARY WAGE EARNERS OVER AGE 24, DERIVES WEEKLY EARNINGS
#              (HOURLY WAGE x HOURS, ROUND 4/2 OR CARRIED FORWARD FROM ROUND 3/1), ASSIGNS WEIGHTED
#              EARNINGS QUARTILES, AND ESTIMATES MEAN PERCEIVED HEALTH STATUS BY QUARTILE.
#
# INPUT FILE:  C:/MEPS/h70.ssp (2002 FULL-YEAR CONSOLIDATED FILE). The SAS program used H62, a
#              preliminary 2002 file with preliminary weights (PERWT02P) that is no longer
#              distributed; the final file and weight (PERWT02F) are used instead, so estimates
#              differ from EM1.pdf (see Python/VALIDATION.md).
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[3]))
from meps_utils import meps_path, read_meps, survey_estimate

HLTHF = {1: "1 EXCELLENT", 2: "2 VERY GOOD", 3: "3 GOOD", 4: "4 FAIR", 5: "5 POOR"}
QUARTF = {1: "1 LOWEST", 2: "2", 3: "3", 4: "4 HIGHEST"}
WKLY_BINS = [0.13, 49.99, 500, 1000, np.inf]
WKLY_LABELS = ["0.13 - 49.99", "50.00 - 499.99", "500.00 - 999.99", "1,000.00+"]
TOPCODE_WAGE = 61.98      # value SAS assigns to top-coded hourly wages (HRWG42X = -10)
WEIGHT = "PERWT02F"


def main(meps_dir=None):
    fy = read_meps(meps_path("h70.ssp", meps_dir),
                   columns=["DUPERSID", WEIGHT, "VARSTR", "VARPSU", "EMPST42", "SELFCM42", "SELFCM31", "HRWG42X",
                            "HRWG31X", "HOUR42", "HOUR31", "RTHLTH42", "TEMPJB42", "AGE42X"])
    keep = (fy["EMPST42"].isin([1, 2])
            & ((fy["SELFCM42"] == 2) | ((fy["SELFCM42"] == -2) & (fy["SELFCM31"] == 2)))
            & ((fy["HRWG42X"] == -10) | (fy["HRWG42X"] > 0)
               | ((fy["HRWG42X"] == -2) & ((fy["HRWG31X"] == -10) | (fy["HRWG31X"] > 0))))
            & ((fy["HOUR42"] > 0) | ((fy["HOUR42"] == -2) & (fy["HOUR31"] > 0)))
            & fy["RTHLTH42"].isin([1, 2, 3, 4, 5]) & (fy["TEMPJB42"] == 2) & (fy["AGE42X"] > 24))
    # the SAS WHERE clause keeps only this population; the full file stays in the design (subpop)
    wage = fy["HRWG42X"].where(fy["HRWG42X"] != -2, fy["HRWG31X"]).replace(-10, TOPCODE_WAGE)
    hours = fy["HOUR42"].where(fy["HOUR42"] > 0, fy["HOUR31"].where(fy["HOUR42"] == -2))
    fy["WKLYEARN"] = np.where(keep, wage * hours, np.nan)

    # weighted quartiles: cumulative weight over earners sorted by weekly earnings
    earn = fy[keep].sort_values("WKLYEARN", kind="stable")
    share = earn[WEIGHT].cumsum() / earn[WEIGHT].sum()
    fy.loc[earn.index, "QUARTILE"] = np.select([share <= 0.25, share <= 0.5, share <= 0.75], [1, 2, 3], 4)
    earn = fy[keep]

    w_quart = earn.groupby(earn["QUARTILE"].map(QUARTF))[WEIGHT].sum()
    wkly = pd.cut(earn["WKLYEARN"], WKLY_BINS, right=False, labels=WKLY_LABELS)
    print(f"weighted QUARTILE:\n{w_quart.to_string()}")
    print(f"weighted WKLYEARN:\n{earn.groupby(wkly, observed=False)[WEIGHT].sum().to_string()}")
    print(f"unweighted WKLYEARN:\n{wkly.value_counts(sort=False).to_string()}")
    print(earn.groupby(["QUARTILE", earn["RTHLTH42"].map(HLTHF)]).size().to_string())
    pct = (earn.groupby(["QUARTILE", "RTHLTH42"])[WEIGHT].sum()
           / earn.groupby("QUARTILE")[WEIGHT].sum() * 100).unstack().rename(index=QUARTF, columns=HLTHF)
    print(pct.round(2).to_string())

    fy["QUARTILE"] = fy["QUARTILE"].map(QUARTF)
    design = {"weight": WEIGHT, "subpop": pd.Series(keep, index=fy.index)}
    results = {"health_overall": survey_estimate(fy, "RTHLTH42", stat="mean", **design),
               "health_by_quartile": survey_estimate(fy.fillna({"QUARTILE": "none"}), "RTHLTH42", stat="mean",
                                                     domain="QUARTILE", **design)}
    results["health_by_quartile"] = results["health_by_quartile"].query("QUARTILE != 'none'")
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
