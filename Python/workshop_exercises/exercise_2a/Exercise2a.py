# -----------------------------------------------------------------------------
# Python port of SAS/workshop_exercises/exercise_2a/Exercise2a.sas
# (no R counterpart; validated against Exercise2a_OUTPUT.TXT)
#
# PURPOSE: SELECTED ESTIMATES FOR A 2015 VERSION OF MEPS STATISTICAL BRIEF #275:
#          "Trends in Antipsychotics Purchases and Expenses for the U.S. Civilian
#          Noninstitutionalized Population, 1997 and 2007"
#   (1) TOTAL EXPENSE FOR ANTIPSYCHOTICS
#   (2) TOTAL NUMBER OF PURCHASES OF ANTIPSYCHOTICS
#   (3) TOTAL NUMBER OF PERSONS PURCHASING ONE OR MORE ANTIPSYCHOTICS
#   (4) AVERAGE TOTAL, OUT OF POCKET, AND THIRD PARTY PAYER EXPENSE
#       FOR ANTIPSYCHOTICS PER PERSON WITH A PURCHASE
#
# INPUT FILES: (1) C:/MEPS/h181.ssp (2015 FULL-YEAR CONSOLIDATED PUF)
#              (2) C:/MEPS/h178a.ssp (2015 PRESCRIBED MEDICINES PUF)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))
from meps_utils import meps_path, read_meps, survey_estimate

VARS = ["n_purchase", "tot", "oop", "third_payer"]


def main(meps_dir=None):
    fyc = read_meps(meps_path("h181.ssp", meps_dir), columns=["DUPERSID", "VARSTR", "VARPSU", "PERWT15F"])
    rx = read_meps(meps_path("h178a.ssp", meps_dir))

    # 1) Identify antipsychotic drugs using therapeutic classification (TC) codes
    drug = rx.loc[(rx["TC1"] == 242) & (rx["TC1S1"] == 251), ["DUPERSID", "RXRECIDX", "LINKIDX", "TC1", "TC1S1", "RXXP15X", "RXSF15X"]]
    print(drug.head(30))

    # 2) Sum data to person-level
    pers = drug.groupby("DUPERSID").agg(tot=("RXXP15X", "sum"), oop=("RXSF15X", "sum"),
                                        n_purchase=("RXXP15X", "size")).reset_index()
    pers["third_payer"] = pers["tot"] - pers["oop"]
    pers["sub"] = 1

    # 3) Merge person-level expenditures to the FY PUF; persons without a purchase get zeros
    fy = fyc.merge(pers, on="DUPERSID", how="left")
    fy[VARS] = fy[VARS].fillna(0)
    fy["sub"] = fy["sub"].fillna(2).astype(int)
    print(fy.groupby("sub").size())

    # 4) Estimates on expenditures and use, for persons with 1+ purchase (domain SUB=1)
    w = {"weight": "PERWT15F"}
    sub = fy["sub"] == 1
    results = {
        "mean_all": survey_estimate(fy, VARS, stat="mean", **w),
        "total_all": survey_estimate(fy, VARS, stat="total", **w),
        "mean_sub": survey_estimate(fy, VARS, stat="mean", subpop=sub, **w),
        "total_sub": survey_estimate(fy, VARS, stat="total", subpop=sub, **w),
    }
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
