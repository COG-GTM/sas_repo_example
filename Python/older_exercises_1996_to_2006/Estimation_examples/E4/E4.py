# -----------------------------------------------------------------------------
# Python port of SAS/older_exercises_1996_to_2006/Estimation_examples/E4/E4.sas
# (validated against the SAS listing in E4.pdf)
#
# DESCRIPTION: THIS EXAMPLE SHOWS HOW TO COMPUTE FAMILY-LEVEL ESTIMATES (MEPS
#              ANNUALIZED FAMILY, FAMILY ID = DUID + FAMIDYR, WEIGHT FAMWT01F):
#              MEAN FAMILY SIZE AND MEAN TOTAL FAMILY EXPENDITURES, OVERALL AND
#              BY FAMILY SIZE.
#
# INPUT FILE:  C:/MEPS/h60.ssp (2001 FULL-YEAR DATA FILE)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np

sys.path.append(str(Path(__file__).resolve().parents[3]))
from meps_utils import meps_path, read_meps, survey_estimate

DESIGN = {"stratum": "VARSTR01", "psu": "VARPSU01", "weight": "FAMWT01F"}


def main(meps_dir=None):
    h60 = read_meps(meps_path("h60.ssp", meps_dir),
                    columns=["DUID", "FAMIDYR", "DUPERSID", "FAMWT01F", "VARSTR01", "VARPSU01", "FAMRFPYR",
                             "FAMSZEYR", "TOTEXP01"])
    h60["DUIDFAMY"] = h60["DUID"].astype(int).astype(str).str.zfill(5) + h60["FAMIDYR"].str.strip()
    h60 = h60.sort_values("DUIDFAMY", kind="stable")

    # Family total expenditures; one record per family (the last member), positive family weight
    h60["FAMTOT01"] = h60.groupby("DUIDFAMY")["TOTEXP01"].transform("sum")
    fam = h60.groupby("DUIDFAMY").tail(1)
    fam = fam[fam["FAMWT01F"] > 0].copy()
    print(fam["FAMSZEYR"].value_counts().sort_index().to_string())
    print(fam.loc[fam["DUIDFAMY"].isin(["40001A", "40006A", "40007A", "40010A", "40011A"]),
                  ["DUIDFAMY", "FAMTOT01"]].to_string(index=False))

    fam["FAMF"] = np.where(fam["FAMSZEYR"] >= 5, "5+", fam["FAMSZEYR"].astype(int).astype(str))
    results = {
        "mean_fam": survey_estimate(fam, ["FAMSZEYR", "FAMTOT01"], stat="mean", **DESIGN),
        "mean_famtot_by_size": survey_estimate(fam, "FAMTOT01", stat="mean", domain="FAMF", **DESIGN),
    }
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
