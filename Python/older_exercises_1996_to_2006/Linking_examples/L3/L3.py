# -----------------------------------------------------------------------------
# Python port of SAS/older_exercises_1996_to_2006/Linking_examples/L3/L3.sas
# (validated against the SUDAAN/SAS listing in L3.pdf)
#
# DESCRIPTION: (1) AGGREGATE OFFICE-BASED VISIT RECORDS TO THE PERSON LEVEL (ANY GENERAL CHECKUP,
#                  AMOUNT PAID IN TOTAL AND BY FAMILY FOR CHECKUPS)
#              (2) MAKE 1 ANNUAL AGE VARIABLE FROM 3 ROUND VARIABLES
#              (3) MAKE A CATEGORICAL VARIABLE FROM A CONTINUOUS VARIABLE
#              (4) ESTIMATE THE PERCENT OF ADULTS WITH A GENERAL CHECKUP, OVERALL AND BY
#                  INSURANCE STATUS (SUDAAN PROC CROSSTAB IN THE ORIGINAL)
#
# INPUT FILES: C:/MEPS/h59g.ssp (2001 OFFICE-BASED VISITS), C:/MEPS/h60.ssp (2001 FULL-YEAR FILE)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np

sys.path.append(str(Path(__file__).resolve().parents[3]))
from meps_utils import meps_path, read_meps, survey_estimate

AGECAT = {1: "1. 0-3", 2: "2. 4-7", 3: "3. 8-11", 4: "4. 12-15", 5: "5. 16-17", 6: "6. 18+"}
INSURED = {1: "1 Insured", 2: "2 Uninsured"}
GENCKUP = {1: "1 General Checkup", 2: "2 No General Checkup"}
DESIGN = {"weight": "PERWT01F", "stratum": "VARSTR01", "psu": "VARPSU01"}


def main(meps_dir=None):
    ob = read_meps(meps_path("h59g.ssp", meps_dir), columns=["DUPERSID", "VSTCTGRY", "OBXP01X", "OBSF01X"])
    ck = ob[ob["VSTCTGRY"] == 1]
    h59g = ck.groupby("DUPERSID", as_index=False).agg(AMBTOTPD=("OBXP01X", "sum"), AMBFAMPD=("OBSF01X", "sum"))
    h59g["GENCKUP"] = 1

    fy = read_meps(meps_path("h60.ssp", meps_dir),
                   columns=["DUPERSID", "PERWT01F", "VARSTR01", "VARPSU01", "AGE31X", "AGE42X", "AGE53X", "INSCOV01"])
    fy = fy[fy["PERWT01F"] > 0].copy()
    age = fy["AGE53X"].where(fy["AGE53X"] >= 0, fy["AGE42X"].where(fy["AGE42X"] >= 0,
                                                                    fy["AGE31X"].where(fy["AGE31X"] >= 0)))
    fy["AGECAT"] = sum((age >= 0, age > 3, age > 7, age > 11, age > 15, age > 17)).astype(int)
    fy["INSURED"] = np.where(fy["INSCOV01"] > 2, 2, 1)
    print(fy["AGECAT"].map(AGECAT).value_counts().sort_index().to_string())

    pers = fy.drop(columns=["AGE31X", "AGE42X", "AGE53X", "INSCOV01"]).merge(h59g, on="DUPERSID", how="left")
    pers["GENCKUP"] = pers["GENCKUP"].fillna(2).astype(int)
    print(pers["GENCKUP"].map(GENCKUP).value_counts().sort_index().to_string())
    for k, lab in GENCKUP.items():
        pers[lab] = (pers["GENCKUP"] == k).astype(float)
    pers["INSURED"] = pers["INSURED"].map(INSURED)

    adult = pers["AGECAT"] == 6                                     # SUBPOPN AGECAT=6
    cols = list(GENCKUP.values())
    overall = survey_estimate(pers, cols, stat="mean", subpop=adult, **DESIGN)
    by_ins = survey_estimate(pers, cols, stat="mean", subpop=adult, domain="INSURED", **DESIGN)
    for res in (overall, by_ins):
        res[["estimate", "se"]] *= 100

    # PROC MEANS (weighted, no design SEs) for adults with a general checkup
    sub = pers[adult & (pers["GENCKUP"] == 1)]
    paid = (sub.groupby("INSURED")[["AMBTOTPD", "AMBFAMPD"]]
            .apply(lambda g: g.mul(sub.loc[g.index, "PERWT01F"], axis=0).sum() / sub.loc[g.index, "PERWT01F"].sum())
            .reset_index().melt(id_vars="INSURED", value_name="estimate"))
    results = {"genckup": overall, "genckup_by_insured": by_ins, "paid_by_insured": paid}
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
