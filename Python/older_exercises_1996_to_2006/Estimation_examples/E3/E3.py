# -----------------------------------------------------------------------------
# Python port of SAS/older_exercises_1996_to_2006/Estimation_examples/E3/E3.sas
# (validated against the SAS listing in E3.pdf)
#
# DESCRIPTION: THIS EXAMPLE SHOWS HOW TO COMPUTE LONGITUDINAL ESTIMATES FOR PANEL 4
#              (1999-2000) USING THE PANEL 4 LONGITUDINAL WEIGHT (LONGWTP4):
#              PERCENT UNINSURED IN 2000 BY 1999 INSURANCE STATUS, AND PERCENT WITH
#              ANY EXPENSE IN 2000 BY WHETHER THE PERSON HAD AN EXPENSE IN 1999.
#
# INPUT FILES: C:/MEPS/h38.ssp (1999 FULL-YEAR FILE)
#              C:/MEPS/h50.ssp (2000 FULL-YEAR FILE)
#              C:/MEPS/h58.ssp (PANEL 4 LONGITUDINAL FILE)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np

sys.path.append(str(Path(__file__).resolve().parents[3]))
from meps_utils import meps_path, read_meps, survey_estimate

DESIGN = {"stratum": "VARSTRP4", "psu": "VARPSUP4", "weight": "LONGWTP4"}


def main(meps_dir=None):
    h38 = read_meps(meps_path("h38.ssp", meps_dir), columns=["DUPERSID", "UNINS99", "TOTEXP99", "PANEL99"])
    h50 = read_meps(meps_path("h50.ssp", meps_dir), columns=["DUPERSID", "UNINS00", "TOTEXP00", "PANEL00"])
    h58 = read_meps(meps_path("h58.ssp", meps_dir), columns=["DUPERSID", "LONGWTP4", "VARPSUP4", "VARSTRP4", "YRINDP4"])

    # Panel 4 persons present in both years (defines the longitudinal file, not a domain)
    long = (h38[h38["PANEL99"] == 4].drop(columns="PANEL99")
            .merge(h50[h50["PANEL00"] == 4].drop(columns="PANEL00"), on="DUPERSID")
            .merge(h58[h58["YRINDP4"] == 1].drop(columns="YRINDP4"), on="DUPERSID"))
    long["UNINS99"] = (long["UNINS99"] == 1).astype(int)
    long["UNINS00"] = np.where(long["UNINS00"] == 1, 100, 0)     # 100 -> mean reads as a percent
    long["TOTEXP99"] = (long["TOTEXP99"] > 0).astype(int)
    long["TOTEXP00"] = np.where(long["TOTEXP00"] > 0, 100, 0)

    results = {
        "pct_unins00": survey_estimate(long, "UNINS00", stat="mean", **DESIGN),
        "pct_unins00_by_unins99": survey_estimate(long, "UNINS00", stat="mean", domain="UNINS99", **DESIGN),
        "pct_exp00": survey_estimate(long, "TOTEXP00", stat="mean", **DESIGN),
        "pct_exp00_by_exp99": survey_estimate(long, "TOTEXP00", stat="mean", domain="TOTEXP99", **DESIGN),
    }
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
