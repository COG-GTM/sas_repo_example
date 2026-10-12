# -----------------------------------------------------------------------------
# Python port of SAS/workshop_exercises/exercise_2b/Exercise2b.sas
# (R reference: R/workshop_exercises/exercise_2a.R)
#
# PURPOSE: SELECTED ESTIMATES FOR A 2016 VERSION OF PURCHASES AND EXPENSES FOR
#          NARCOTIC ANALGESICS OR NARCOTIC ANALGESIC COMBOS
#   (1) TOTAL EXPENSE FOR NARCOTIC ANALGESICS OR NARCOTIC ANALGESIC COMBOS
#   (2) TOTAL NUMBER OF PURCHASES OF NARCOTIC ANALGESICS OR NARCOTIC ANALGESIC COMBOS
#   (3) TOTAL NUMBER OF PERSONS PURCHASING ONE OR MORE NARCOTIC ANALGESICS OR NARCOTIC ANALGESIC COMBOS
#   (4) AVERAGE TOTAL, OUT OF POCKET, AND THIRD PARTY PAYER EXPENSE
#       FOR NARCOTIC ANALGESICS OR NARCOTIC ANALGESIC COMBOS PER PERSON WITH A PURCHASE
#
# INPUT FILES: (1) C:/MEPS/h192.ssp (2016 FULL-YEAR CONSOLIDATED PUF)
#              (2) C:/MEPS/h188a.ssp (2016 PRESCRIBED MEDICINES PUF)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))
from meps_utils import meps_path, read_meps, survey_estimate

VARS = ["n_purchase", "tot", "oop", "third_payer"]


def main(meps_dir=None):
    fyc = read_meps(meps_path("h192.ssp", meps_dir), columns=["DUPERSID", "VARSTR", "VARPSU", "PERWT16F"])
    rx = read_meps(meps_path("h188a.ssp", meps_dir))

    # 1) Identify narcotic analgesics or narcotic analgesic combos using therapeutic classification (TC) codes
    drug = rx.loc[rx["TC1S1_1"].isin([60, 191]), ["DUPERSID", "RXRECIDX", "LINKIDX", "TC1S1_1", "RXXP16X", "RXSF16X"]]
    print(drug.head(30))

    # 2) Sum data to person-level
    pers = drug.groupby("DUPERSID").agg(tot=("RXXP16X", "sum"), oop=("RXSF16X", "sum"),
                                        n_purchase=("RXXP16X", "size")).reset_index()
    pers["third_payer"] = pers["tot"] - pers["oop"]
    pers["sub"] = 1

    # 3) Merge person-level expenditures to the FY PUF; persons without a purchase get zeros
    fy = fyc.merge(pers, on="DUPERSID", how="left")
    fy[VARS] = fy[VARS].fillna(0)
    fy["sub"] = fy["sub"].fillna(2).astype(int)
    print(fy.groupby("sub").size())

    # 4) Estimates on expenditures and use, for persons with 1+ purchase (domain SUB=1)
    w = {"weight": "PERWT16F"}
    sub = fy["sub"] == 1
    results = {
        "mean_sub": survey_estimate(fy, VARS, stat="mean", subpop=sub, **w),
        "total_sub": survey_estimate(fy, VARS, stat="total", subpop=sub, **w),
    }
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
