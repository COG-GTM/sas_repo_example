# -----------------------------------------------------------------------------
# Example code to replicate estimates from the MEPS-HC Data Tools summary tables
#
# Prescribed drugs, 2016
#
# Purchases and expenditures by generic drug name (RXDRGNAM)
#  - Number of people with purchase
#  - Total purchases
#  - Total expenditures
#
# Input file: C:/MEPS/h188a.ssp (2016 RX event file, SAS XPORT)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from meps_utils import meps_path, read_meps, survey_estimate


def main(meps_dir=None):
    # Load datasets -------------------------------------------------------------
    # For 1996-2013, need to merge with RX Multum Lexicon Addendum files to get
    #  therapeutic class categories and generic drug names
    rx = read_meps(meps_path("h188a.ssp", meps_dir))

    # Aggregate to person-level (PROC SORT + PROC MEANS BY ... -> groupby/agg) --
    rx_pers = (
        rx.groupby(["DUPERSID", "VARSTR", "VARPSU", "RXDRGNAM"], as_index=False)
        .agg(PERWT16F=("PERWT16F", "mean"),
             pers_RXXP=("RXXP16X", "sum"),
             n_purchases=("RXXP16X", "size"))
        .assign(persons=1)
    )

    # Calculate estimates (domain = generic drug name) --------------------------
    totals = survey_estimate(
        rx_pers, ["persons", "n_purchases", "pers_RXXP"],
        weight="PERWT16F", stat="total", domain="RXDRGNAM",
    )

    labels = {"persons": "Number of people with purchase",
              "n_purchases": "Total purchases",
              "pers_RXXP": "Total expenditures"}
    for var, label in labels.items():
        print(f"\n{label}:")
        print(totals.loc[totals.variable == var, ["RXDRGNAM", "estimate", "se"]].to_string(index=False))
    return {"totals": totals}


if __name__ == "__main__":
    main()
