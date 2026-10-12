# -----------------------------------------------------------------------------
# Python port of SAS/workshop_exercises/exercise_5a/Exercise5a.sas
# (no R counterpart; validated against Exercise5a_OUTPUT.TXT)
#
# DESCRIPTION: THIS PROGRAM ILLUSTRATES HOW TO CONSTRUCT FAMILY-LEVEL VARIABLES FROM
#              PERSON-LEVEL DATA. THERE ARE TWO DEFINITIONS OF FAMILY UNIT IN MEPS.
#                1) CPS FAMILY:  ID IS DUID + CPSFAMID. CORRESPONDING WEIGHT IS FAMWT15C.
#                2) MEPS FAMILY: ID IS DUID + FAMIDYR.  CORRESPONDING WEIGHT IS FAMWT15F.
#              THE CPS FAMILY IS USED IN THIS EXERCISE.
#
# INPUT FILE:  C:/MEPS/h181.ssp (2015 FY PUF DATA)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))
from meps_utils import meps_path, read_meps, survey_estimate

FAM_ID = ["DUID", "CPSFAMID"]


def main(meps_dir=None):
    pers = read_meps(meps_path("h181.ssp", meps_dir),
                     columns=["DUPERSID", "DUID", "CPSFAMID", "FAMWT15C", "VARSTR", "VARPSU",
                              "TOTSLF15", "TTLP15X"])
    pers = pers.sort_values(FAM_ID, kind="stable")

    # Family size, total out-of-pocket expenditure and total income per CPS family
    fam = pers.groupby(FAM_ID).agg(FAMSIZE=("DUPERSID", "size"), FAMOOP=("TOTSLF15", "sum"),
                                   FAMINC=("TTLP15X", "sum")).reset_index()
    print(fam.head(20).to_string(index=False))

    # Family weight and design variables (first person with a positive family weight)
    famwt = pers.loc[pers["FAMWT15C"] > 0, FAM_ID + ["FAMWT15C", "VARSTR", "VARPSU"]].drop_duplicates(FAM_ID)
    fam2 = fam.merge(famwt, on=FAM_ID, how="inner")

    results = {"mean_fam": survey_estimate(fam2, ["FAMSIZE", "FAMOOP", "FAMINC"], weight="FAMWT15C",
                                           stat="mean")}
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
