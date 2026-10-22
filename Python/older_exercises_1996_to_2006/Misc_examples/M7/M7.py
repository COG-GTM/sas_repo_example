# -----------------------------------------------------------------------------
# Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M7/M7.sas
# (M7.pdf contains only the SAS log, so there are no printed values to validate against)
#
# DESCRIPTION: 2003 PRESCRIBED MEDICINE EXPENDITURES FOR PERSONS WITH A PMED PURCHASE:
#              DESIGN-BASED MEAN/TOTAL, AND WEIGHTED TOTALS (IN $1,000) AND MEANS BY SOURCE OF
#              PAYMENT, OVERALL AND BY AGE GROUP, RACE/ETHNICITY AND INSURANCE STATUS.
#
# INPUT FILES: C:/MEPS/h77a.ssp (2003 PRESCRIBED MEDICINES), C:/MEPS/h79.ssp (2003 FULL-YEAR FILE)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[3]))
from meps_utils import meps_path, read_meps, survey_estimate

AGECAT = {1: "UNDER 18", 2: "18 - 64", 3: "65 AND OLDER"}
RACETHN = {1: "HISPANIC", 2: "BLACK", 3: "ASIAN", 4: "OTHER"}
INSCOV = {1: "ANY PRIVATE", 2: "PUBLIC ONLY", 3: "UNINSURED"}
PAY = ["RXEXP03", "RXSLF03", "RXPRV03", "RXMCR03", "RXMCD03", "OTHER"]


def main(meps_dir=None):
    users = read_meps(meps_path("h77a.ssp", meps_dir), columns=["DUPERSID"]).drop_duplicates()
    fy = read_meps(meps_path("h79.ssp", meps_dir))
    age = fy["AGE03X"].where(fy["AGE03X"] >= 0, fy["AGE53X"].where(fy["AGE53X"] >= 0, fy["AGE42X"].where(
        fy["AGE42X"] >= 0, fy["AGE31X"].where(fy["AGE31X"] >= 0, fy["AGE03X"]))))
    fy["AGECAT"] = pd.Series(np.select([age.between(0, 17.999), age.between(18, 64.999), age >= 65], [1, 2, 3], 0),
                             index=fy.index).map(AGECAT)
    fy["RACETHNX"] = fy["RACETHNX"].map(RACETHN)
    fy["INSCOV03"] = fy["INSCOV03"].map(INSCOV)
    fy["OTHER"] = fy["RXEXP03"] - fy[["RXMCD03", "RXMCR03", "RXPRV03", "RXSLF03"]].sum(axis=1)
    pmed = fy.merge(users, on="DUPERSID")    # persons with a PMED purchase (sample definition)

    results = {"mean_rxexp": survey_estimate(pmed, "RXEXP03", weight="PERWT03F", stat="mean"),
               "total_rxexp": survey_estimate(pmed, "RXEXP03", weight="PERWT03F", stat="total")}
    w = pmed["PERWT03F"]
    for by in [None, "AGECAT", "RACETHNX", "INSCOV03"]:     # PROC TABULATE ... WEIGHT PERWT03F
        g = pmed.assign(_all="TOTAL").groupby(by or "_all")
        sums = g.apply(lambda d: (d[PAY].mul(d["PERWT03F"], axis=0)).sum() / 1000)
        means = sums * 1000 / g["PERWT03F"].sum().to_numpy()[:, None]
        print(f"\nTotal PMED expenditures ($1,000) by {by or 'TOTAL'}:\n{sums.round(0).to_string()}")
        print(f"\nMean PMED expenditures by {by or 'TOTAL'}:\n{means.round(0).to_string()}")
    del w
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
