# -----------------------------------------------------------------------------
# Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M11/M11.sas
# (validated against the SAS listing in M11.pdf)
#
# DESCRIPTION: LINKS CHILDREN (AGE 0-17) TO THEIR MOTHER'S AND FATHER'S RECORDS IN THE SAME
#              DWELLING UNIT (MOPID31X / DAPID31X) TO DERIVE PARENTS' EMPLOYMENT STATUS IN ROUND
#              3/1, THEN ESTIMATES THE DISTRIBUTION OF CHILDREN BY NUMBER OF WORKING PARENTS.
#
# INPUT FILE:  C:/MEPS/h105.ssp (2006 FULL-YEAR FILE)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np

sys.path.append(str(Path(__file__).resolve().parents[3]))
from meps_utils import meps_path, read_meps, survey_freq

KIDFMT = {1: "CHILD, AGE 0-17", 0: "NOT A CHILD"}
PAR_WORK = {1: "1 BOTH PARENTS WORK", 2: "2 ONE PARENT WORK", 3: "3 NO WORKING PARENT", 0: "0 NOT A CHILD"}


def main(meps_dir=None):
    fy = read_meps(meps_path("h105.ssp", meps_dir),
                   columns=["DUPERSID", "DUID", "PID", "AGE31X", "MOPID31X", "DAPID31X", "EMPST31", "PERWT06F",
                            "VARSTR", "VARPSU"])
    fy = fy[fy["PERWT06F"] > 0].copy()
    duid = fy["DUID"].astype(int).astype(str).str.zfill(5)
    fy["MOMLINK"] = np.where(fy["MOPID31X"] != -1, duid + fy["MOPID31X"].astype(int).astype(str).str.zfill(3), None)
    fy["DADLINK"] = np.where(fy["DAPID31X"] != -1, duid + fy["DAPID31X"].astype(int).astype(str).str.zfill(3), None)

    kids = fy.loc[fy["AGE31X"].between(0, 17), ["DUPERSID", "MOMLINK", "DADLINK"]]
    emp = fy.set_index("DUPERSID")["EMPST31"]
    kids["DAD_EMPST31"] = kids["DADLINK"].map(emp).fillna(0)      # 0 = no dad in MEPS
    kids["MOM_EMPST31"] = kids["MOMLINK"].map(emp).fillna(0)      # 0 = no mom in MEPS

    fy = fy.merge(kids[["DUPERSID", "DAD_EMPST31", "MOM_EMPST31"]], on="DUPERSID", how="left", indicator=True)
    is_kid = fy["_merge"] == "both"
    fy["POP_KID"] = is_kid.astype(int).map(KIDFMT)
    both = (fy["MOM_EMPST31"] == 1) & (fy["DAD_EMPST31"] == 1)
    one = (fy["MOM_EMPST31"] == 1) | (fy["DAD_EMPST31"] == 1)
    fy["PAR_WORK"] = np.select([~is_kid, both, one], [0, 1, 2], 3)
    fy["PAR_WORK"] = fy["PAR_WORK"].map(PAR_WORK)
    fy[["DAD_EMPST31", "MOM_EMPST31"]] = fy[["DAD_EMPST31", "MOM_EMPST31"]].where(is_kid, -1)
    print(fy.groupby(["POP_KID", "PAR_WORK", "MOM_EMPST31", "DAD_EMPST31"]).size().to_string())

    # SAS drops nonpositive weights (none left here) and omits strata with a single PSU
    tables = survey_freq(fy, "POP_KID", "PAR_WORK", weight="PERWT06F", lonely_psu="remove")
    results = {f"kid_parwork_{k}": v for k, v in tables.items()}
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
