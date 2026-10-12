# -----------------------------------------------------------------------------
# Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M6/M6.sas
# (validated against the SAS listing in M6.pdf)
#
# DESCRIPTION: ONE-WAY DESIGN-BASED FREQUENCIES (PROC SURVEYFREQ) FOR PERSONS WITH
#              DIABETES, 2005, USING THE DIABETES CARE SUPPLEMENT WEIGHT (DIABW05F):
#              TIMES TESTED FOR HEMOGLOBIN A1C (DSA1C53) AND INSULIN USE (DSINSU53).
#
# INPUT FILE:  C:/MEPS/h97.ssp (2005 FULL-YEAR FILE)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[3]))
from meps_utils import meps_path, read_meps, survey_estimate

SPECIAL = {-9: "-9 NOT ASCERTAINED", -8: "-8 DK", -1: "-1 INAPPLICABLE"}


def a1cf(v):
    lab = np.select([v.between(1, 20), v.between(21, 50), v > 50], ["1-20 TIMES", "21-50 TIMES", "51 TIMES OR MORE"],
                    None)
    return np.where(v.isin(list(SPECIAL)), v.map(SPECIAL), np.where(pd.isna(lab), v.astype(int).astype(str), lab))


def insf(v):
    labels = {**SPECIAL, 1: "1 YES", 0: "0  NO"}
    return v.map(labels).fillna(v.astype(int).astype(str))


def one_way(df, col, weight):
    """SAS PROC SURVEYFREQ TABLES col: weighted frequency and percent (with SEs) of each level."""
    # SAS drops records with nonpositive weights (most of the sample here) before forming the
    # design; strata left with a single PSU are omitted from the variance (lonely_psu="remove").
    df = df[df[weight] > 0]
    in_table = df[weight] > 0
    levels = sorted(df.loc[in_table, col].unique())
    tmp = df[[weight, "VARSTR", "VARPSU"]].copy()
    for lev in levels:
        tmp[lev] = (df[col] == lev).astype(float)
    kw = {"weight": weight, "subpop": in_table, "lonely_psu": "remove"}
    pct = survey_estimate(tmp, levels, stat="mean", **kw)
    pct[["estimate", "se"]] *= 100
    return survey_estimate(tmp, levels, stat="total", **kw), pct


def main(meps_dir=None):
    fy = read_meps(meps_path("h97.ssp", meps_dir), columns=["DUPERSID", "DIABW05F", "VARSTR", "VARPSU", "DSA1C53", "DSINSU53"])
    fy["DSA1C53"] = a1cf(fy["DSA1C53"])
    fy["DSINSU53"] = insf(fy["DSINSU53"])
    results = {}
    for col in ["DSA1C53", "DSINSU53"]:
        results[f"{col}_wgt_freq"], results[f"{col}_percent"] = one_way(fy, col, "DIABW05F")
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
