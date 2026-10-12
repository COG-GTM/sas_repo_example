# -----------------------------------------------------------------------------
# Python port of SAS/workshop_exercises/exercise_4a/Exercise4a.sas
# (R reference: R/workshop_exercises/exercise_3a.R)
#
# DESCRIPTION: THIS PROGRAM ILLUSTRATES HOW TO POOL MEPS DATA FILES FROM DIFFERENT YEARS.
#              THE EXAMPLE USED IS POPULATION AGE 26-30 WHO ARE UNINSURED BUT HAVE HIGH INCOME.
#              VARIABLES WITH YEAR-SPECIFIC NAMES MUST BE RENAMED BEFORE COMBINING FILES.
#
# INPUT FILES: (1) C:/MEPS/h192.ssp (2016 FULL-YEAR FILE)
#              (2) C:/MEPS/h181.ssp (2015 FULL-YEAR FILE)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))
from meps_utils import meps_path, read_meps, survey_estimate

KEEP = ["DUPERSID", "VARSTR", "VARPSU", "AGELAST", "inscov", "perwt", "povcat", "totslf"]


def load_year(fname, yy, meps_dir):
    df = read_meps(meps_path(fname, meps_dir))
    df = df.rename(columns={f"INSCOV{yy}": "inscov", f"PERWT{yy}F": "perwt",
                            f"POVCAT{yy}": "povcat", f"TOTSLF{yy}": "totslf"})
    return df[KEEP]


def main(meps_dir=None):
    pool = pd.concat([load_year("h192.ssp", 16, meps_dir), load_year("h181.ssp", 15, meps_dir)],
                     ignore_index=True)
    pool["poolwt"] = pool["perwt"] / 2     # divide perwt by number of years (2)
    subpop = pool["AGELAST"].between(26, 30) & (pool["povcat"] == 5) & (pool["inscov"] == 3)
    print(pool[subpop].groupby(["AGELAST", "povcat", "inscov"]).size())

    results = {"mean_totslf_subpop": survey_estimate(pool, "totslf", weight="poolwt", stat="mean",
                                                     subpop=subpop)}
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
