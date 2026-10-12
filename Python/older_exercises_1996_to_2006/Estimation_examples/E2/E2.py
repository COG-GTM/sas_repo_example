# -----------------------------------------------------------------------------
# Python port of SAS/older_exercises_1996_to_2006/Estimation_examples/E2/E2.sas
# (validated against the SAS listing in E2.pdf)
#
# DESCRIPTION: THIS EXAMPLE SHOWS HOW TO POOL ANNUAL FILES: MEAN TOTAL EXPENDITURES (ALL
#              PERSONS AND CHILDREN AGE 0-5) FOR 1996-1999, AND FOR THE POOLED 1996-97 AND
#              1998-99 FILES (EXPENDITURES INFLATED TO THE LATER YEAR WITH THE CPI, WEIGHTS
#              DIVIDED BY 2, VARIANCE STRATA/PSUS FROM THE POOLED LINKAGE FILE).
#
# INPUT FILES: C:/MEPS/h12.ssp, h20.ssp, h28.ssp, h38.ssp (1996-1999 FULL-YEAR FILES)
#              C:/MEPS/h36u23.dta (POOLED VARIANCE LINKAGE FILE; the SAS program used the
#              1996-2002 version, H36, with STRA9602/PSU9602)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[3]))
from meps_utils import meps_path, read_meps, survey_estimate

YEARS = {  # file, weight, CPI factor to the later year of each pooled pair
    96: ("h12.ssp", "WTDPER96", 166.6 / 156.9),
    97: ("h20.ssp", "WTDPER97", 166.6 / 160.5),
    98: ("h28.ssp", "WTDPER98", 166.6 / 163.0),
    99: ("h38.ssp", "PERWT99F", 1.0),
}


def agef(age):
    return np.where(age.between(0, 5), "AGE 0-5", "OTHER")


def main(meps_dir=None):
    data, results = {}, {}
    for yy, (fname, wt, cpi) in YEARS.items():
        d = read_meps(meps_path(fname, meps_dir),
                      columns=["DUPERSID", f"TOTEXP{yy}", wt, f"VARPSU{yy}", f"VARSTR{yy}", f"AGE{yy}X"])
        d["TOTEXP"] = d[f"TOTEXP{yy}"] * cpi
        d["AGEF"] = agef(d[f"AGE{yy}X"])
        w = {"weight": wt, "stratum": f"VARSTR{yy}", "psu": f"VARPSU{yy}"}
        results[f"mean_exp_{yy}"] = survey_estimate(d, f"TOTEXP{yy}", stat="mean", **w)
        results[f"mean_exp_{yy}_by_age"] = survey_estimate(d, f"TOTEXP{yy}", stat="mean", domain="AGEF", **w)
        data[yy] = d.rename(columns={wt: "POOLWT", f"AGE{yy}X": "AGE"})[["DUPERSID", "TOTEXP", "AGE", "POOLWT"]]

    link = read_meps(meps_path("h36u23.dta", meps_dir), columns=["DUPERSID", "PANEL", "STRA9623", "PSU9623"])
    link = link[link["PANEL"] <= 4].drop_duplicates("DUPERSID").drop(columns="PANEL")
    w = {"weight": "POOLWT", "stratum": "STRA9623", "psu": "PSU9623"}
    for name, (a, b) in {"9697": (96, 97), "9899": (98, 99)}.items():
        pool = pd.concat([data[a], data[b]], ignore_index=True)
        pool["POOLWT"] = pool["POOLWT"].where(pool["POOLWT"] <= 0, pool["POOLWT"] / 2)
        pool = pool.merge(link, on="DUPERSID", how="inner")
        pool["AGEF"] = agef(pool["AGE"])
        results[f"mean_exp_{name}"] = survey_estimate(pool, "TOTEXP", stat="mean", **w)
        results[f"mean_exp_{name}_by_age"] = survey_estimate(pool, "TOTEXP", stat="mean", domain="AGEF", **w)
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
