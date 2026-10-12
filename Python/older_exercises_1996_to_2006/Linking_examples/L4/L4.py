# -----------------------------------------------------------------------------
# Python port of SAS/older_exercises_1996_to_2006/Linking_examples/L4/L4.sas
# (validated against the SUDAAN/SAS listing in L4.pdf)
#
# DESCRIPTION: (1) IDENTIFY PERSONS WITH ASTHMA (ICD9CODX = '493') ON THE CONDITION FILE
#              (2) SUBSET THE CONDITION FILE TO THE PERSON LEVEL
#              (3) MAKE ONE "HOW ASTHMA AFFECTS OVERALL HEALTH" VARIABLE FROM FIVE ROUND VARIABLES
#              (4) ESTIMATE PERSONS WITH ASTHMA, AND THEIR DISTRIBUTION BY OVERALL HEALTH EFFECT
#                  AND AGE GROUP (SUDAAN PROC CROSSTAB IN THE ORIGINAL)
#
# INPUT FILES: C:/MEPS/h61.ssp (2001 CONDITIONS), C:/MEPS/h60.ssp (2001 FULL-YEAR PERSONS)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np

sys.path.append(str(Path(__file__).resolve().parents[3]))
from meps_utils import meps_path, read_meps, survey_estimate

ASTHMA = {1: "1 Has Asthma", 2: "2 No Asthma"}
OVERALL = {1: "1 Very Serious", 2: "2 Somewhat Serious", 3: "3 Not Very Serious", 4: "4 Not at All",
           5: "5 Missing", 6: "6 No Asthma"}
AGECAT = {1: "1. 0-17", 2: "2. 18+"}
DESIGN = {"weight": "PERWT01F", "stratum": "VARSTR01", "psu": "VARPSU01"}


def _table(pers, labels, name, subpop=None, domain=None):
    """SUDAAN CROSSTAB weighted size and column percent (with SEs) for 0/1 indicator columns."""
    size = survey_estimate(pers, labels, stat="total", subpop=subpop, domain=domain, **DESIGN)
    pct = survey_estimate(pers, labels, stat="mean", subpop=subpop, domain=domain, **DESIGN)
    pct[["estimate", "se"]] *= 100
    return {f"{name}_size": size, f"{name}_pct": pct}


def main(meps_dir=None):
    cond = read_meps(meps_path("h61.ssp", meps_dir),
                     columns=["DUPERSID", "PANEL01", "CONDRN", "ICD9CODX", *[f"OVRALL{i}" for i in range(1, 6)]])
    cond = cond[cond["ICD9CODX"] == "493"].sort_values(["DUPERSID", "CONDRN"], kind="stable")
    asthma = cond.drop_duplicates("DUPERSID").copy()                 # first asthma record per person
    rounds = [f"OVRALL{i}" for i in range(1, 6)]
    asthma["OVERALL"] = asthma[rounds].where(asthma[rounds] > -1).bfill(axis=1).iloc[:, 0].fillna(5)
    print(asthma["OVERALL"].map(OVERALL).value_counts().sort_index().to_string())

    fy = read_meps(meps_path("h60.ssp", meps_dir),
                   columns=["DUPERSID", "PERWT01F", "VARSTR01", "VARPSU01", "AGE31X", "AGE42X", "AGE53X"])
    pers = fy.merge(asthma[["DUPERSID", "OVERALL"]], on="DUPERSID", how="left", indicator=True)
    pers = pers[pers["PERWT01F"] > 0].copy()
    pers["ASTHMA"] = np.where(pers["_merge"] == "both", 1, 2)
    pers["OVERALL"] = pers["OVERALL"].fillna(6)
    age = pers["AGE53X"].where(pers["AGE53X"] >= 0, pers["AGE42X"].where(pers["AGE42X"] >= 0,
                                                                        pers["AGE31X"].where(pers["AGE31X"] >= 0)))
    pers["AGECAT"] = np.select([age.between(0, 17.999), age >= 18], [1, 2], 0)
    pers["AGECAT"] = pers["AGECAT"].map(AGECAT)
    for var, fmt in (("ASTHMA", ASTHMA), ("OVERALL", OVERALL)):
        for k, lab in fmt.items():
            pers[lab] = (pers[var] == k).astype(float)

    has = pers["ASTHMA"] == 1                                         # SUBPOPN ASTHMA=1
    overall = [OVERALL[k] for k in range(1, 6)]
    results = {**_table(pers, list(ASTHMA.values()), "asthma"),
               **_table(pers, overall, "overall", subpop=has),
               **_table(pers, overall, "overall_age", subpop=has, domain="AGECAT")}
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
