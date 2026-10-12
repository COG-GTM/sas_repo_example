# -----------------------------------------------------------------------------
# Python port of SAS/workshop_exercises/exercise_5b/Exercise5b.sas
# (no R counterpart; validated against Exercise5b_OUTPUT.TXT)
#
# DESCRIPTION: THIS PROGRAM ILLUSTRATES HOW TO CONSTRUCT INSURANCE STATUS VARIABLES
#              FROM MONTHLY INSURANCE VARIABLES IN THE PERSON-LEVEL DATA
#              (e.g. PRImm15, INSmm15X, MCDmm15X, PEGmm15 ... where mm = JA-DE).
#
# INPUT FILE:  C:/MEPS/h181.ssp (2015 FY PUF DATA)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np

sys.path.append(str(Path(__file__).resolve().parents[2]))
from meps_utils import meps_path, read_meps, survey_estimate

YR = "15"
MONTHS = ["JA", "FE", "MA", "AP", "MY", "JU", "JL", "AU", "SE", "OC", "NO", "DE"]
RACETHX = {1: "1 HISPANIC", 2: "2 WHITE", 3: "3 BLACK", 4: "4 ASIAN", 5: "5 OTHER RACE"}
EDITED = {"INS", "MCD", "MCR", "TRI"}   # monthly variables with an 'X' (edited) suffix
VARS = ["FULL_INSU", "GROUP_INS1", "GROUP_INS2", "NG_INS"]


def monthly(fy, prefix):
    """12-column array of monthly coverage indicators (ARRAY statement)."""
    sfx = "X" if prefix in EDITED else ""
    return fy[[f"{prefix}{m}{YR}{sfx}" for m in MONTHS]].to_numpy()


def main(meps_dir=None):
    fy = read_meps(meps_path("h181.ssp", meps_dir))
    a = {p: monthly(fy, p) for p in
         ["PEG", "POU", "PDK", "PNG", "POG", "PRS", "PRX", "PRI", "INS", "MCD", "MCR", "TRI", "OPA", "OPB"]}

    def months(cond):
        return cond.sum(axis=1)

    fy["PRI_N"] = months(a["PRI"] == 1)
    fy["INS_N"] = months(a["INS"] == 1)
    fy["UNINS_N"] = months(a["INS"] == 2)
    fy["MCD_N"] = months(a["MCD"] == 1)
    fy["MCR_N"] = months(a["MCR"] == 1)
    fy["TRI_N"] = months(a["TRI"] == 1)
    fy["OPAB_N"] = months((a["OPA"] == 1) | (a["OPB"] == 1))
    fy["GRP_N"] = months((a["PEG"] == 1) | (a["TRI"] == 1) | (a["POU"] == 1) | (a["PDK"] == 1))
    fy["NG_N"] = months((a["PRX"] == 1) | (a["PNG"] == 1) | (a["POG"] == 1) | (a["PRS"] == 1))
    fy["PUB_N"] = months((a["MCR"] == 1) | (a["MCD"] == 1) | (a["OPA"] == 1) | (a["OPB"] == 1))
    fy["REF_N"] = months(a["INS"] > 0)
    for v in ["PRI_N", "INS_N", "UNINS_N", "GRP_N", "NG_N", "REF_N"]:
        print(fy[v].value_counts().sort_index().to_string())

    fy["FULL_INSU"] = (fy["UNINS_N"] == 0).astype(int)
    fy["GROUP_INS1"] = (fy["GRP_N"] > 0).astype(int)
    fy["GROUP_INS2"] = ((fy["GRP_N"] > 0) & (fy["GRP_N"] == fy["REF_N"])).astype(int)
    fy["NG_INS"] = (fy["NG_N"] > 0).astype(int)
    fy["RACETHX"] = fy["RACETHX"].map(RACETHX).astype(object).where(fy["RACETHX"].isin(RACETHX), np.nan)

    w = {"weight": "PERWT15F"}
    results = {
        "mean_ins": survey_estimate(fy, VARS, stat="mean", **w),
        "total_ins": survey_estimate(fy, VARS, stat="total", **w),
        "mean_ins_by_race": survey_estimate(fy, VARS, stat="mean", domain="RACETHX", **w),
        "total_ins_by_race": survey_estimate(fy, VARS, stat="total", domain="RACETHX", **w),
    }
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
