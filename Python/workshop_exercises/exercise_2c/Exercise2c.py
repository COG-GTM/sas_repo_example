# -----------------------------------------------------------------------------
# Python port of SAS/workshop_exercises/exercise_2c/Exercise2c.sas
# (R reference: R/workshop_exercises/exercise_2b.R)
#
# PURPOSE: SELECTED ESTIMATES FOR A 2018 VERSION OF PURCHASES AND EXPENSES FOR
#          NARCOTIC ANALGESICS OR NARCOTIC ANALGESIC COMBOS
#   (1) TOTAL EXPENSE FOR NARCOTIC ANALGESICS OR NARCOTIC ANALGESIC COMBOS
#   (2) TOTAL NUMBER OF PURCHASES OF NARCOTIC ANALGESICS OR NARCOTIC ANALGESIC COMBOS
#   (3) TOTAL NUMBER OF PERSONS PURCHASING ONE OR MORE NARCOTIC ANALGESICS OR NARCOTIC ANALGESIC COMBOS
#   (4) AVERAGE TOTAL, OUT OF POCKET, AND THIRD PARTY PAYER EXPENSE
#       FOR NARCOTIC ANALGESICS OR NARCOTIC ANALGESIC COMBOS PER PERSON WITH A PURCHASE
#
# INPUT FILES: (1) C:/MEPS/h209.dta (2018 FULL-YEAR CONSOLIDATED PUF)
#              (2) C:/MEPS/h206a.dta (2018 PRESCRIBED MEDICINES PUF)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))
from meps_utils import meps_path, read_meps, survey_estimate

VARS = ["n_purchase", "tot", "oop", "third_payer"]


def main(meps_dir=None):
    fyc = read_meps(meps_path("h209.dta", meps_dir), columns=["DUPERSID", "VARSTR", "VARPSU", "PERWT18F"])
    rx = read_meps(meps_path("h206a.dta", meps_dir))

    # 1) Identify narcotic analgesics or narcotic analgesic combos using therapeutic classification (TC) codes
    drug = rx.loc[rx["TC1S1_1"].isin([60, 191]), ["DUPERSID", "RXRECIDX", "LINKIDX", "TC1S1_1", "RXXP18X", "RXSF18X"]]
    print(drug.head(30))

    # 2) Sum data to person-level
    pers = drug.groupby("DUPERSID").agg(tot=("RXXP18X", "sum"), oop=("RXSF18X", "sum"),
                                        n_purchase=("RXXP18X", "size")).reset_index()
    pers["third_payer"] = pers["tot"] - pers["oop"]
    pers["sub"] = 1

    # 3) Merge person-level expenditures to the FY PUF; persons without a purchase get zeros
    fy = fyc.merge(pers, on="DUPERSID", how="left")
    fy[VARS] = fy[VARS].fillna(0)
    fy["sub"] = fy["sub"].fillna(2).astype(int)
    print(fy.groupby("sub").size())

    # 4) Estimates on expenditures and use, for persons with 1+ purchase (domain SUB=1)
    w = {"weight": "PERWT18F"}
    sub = fy["sub"] == 1
    results = {
        "total_sub": survey_estimate(fy, VARS, stat="total", subpop=sub, **w),
        "mean_sub": survey_estimate(fy, VARS, stat="mean", subpop=sub, **w),
    }
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
