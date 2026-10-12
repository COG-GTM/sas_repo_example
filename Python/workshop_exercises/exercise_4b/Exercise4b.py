# -----------------------------------------------------------------------------
# Python port of SAS/workshop_exercises/exercise_4b/Exercise4b.sas
# (R reference: R/workshop_exercises/exercise_3b.R)
#
# DESCRIPTION: THIS PROGRAM ILLUSTRATES HOW TO POOL MEPS LONGITUDINAL DATA FILES FROM
#              DIFFERENT PANELS. THE EXAMPLE USED IS PANELS 17-19, POPULATION AGE 26-30
#              WHO ARE UNINSURED BUT HAVE HIGH INCOME IN THE FIRST YEAR.
#
# INPUT FILES: (1) C:/MEPS/h183.ssp (PANEL 19 LONGITUDINAL FILE)
#              (2) C:/MEPS/h172.ssp (PANEL 18 LONGITUDINAL FILE)
#              (3) C:/MEPS/h164.ssp (PANEL 17 LONGITUDINAL FILE)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))
from meps_utils import meps_path, read_meps, survey_estimate

VARLIST = ["DUPERSID", "INSCOVY1", "INSCOVY2", "LONGWT", "VARSTR", "VARPSU", "POVCATY1", "AGEY1X", "PANEL"]


def main(meps_dir=None):
    pool = pd.concat([read_meps(meps_path(f, meps_dir), columns=VARLIST)
                      for f in ("h183.ssp", "h172.ssp", "h164.ssp")], ignore_index=True)
    pool["poolwt"] = pool["LONGWT"] / 3
    subpop = pool["AGEY1X"].between(26, 30) & (pool["POVCATY1"] == 5) & (pool["INSCOVY1"] == 3)
    print(pool[subpop].describe())

    # Distribution of year-2 insurance coverage (1=any private, 2=public only, 3=uninsured)
    levels = sorted(pool.loc[subpop, "INSCOVY2"].unique())
    names = []
    for k in levels:
        name = f"as.factor(INSCOVY2){int(k)}"
        pool[name] = (pool["INSCOVY2"] == k).astype(int)
        names.append(name)

    results = {"pct_inscovy2_subpop": survey_estimate(pool, names, weight="poolwt", stat="mean",
                                                      subpop=subpop)}
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
