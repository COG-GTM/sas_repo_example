"""
Example code to replicate estimates from the MEPS-HC Data Tools summary tables

Health Insurance: Coverage by Age Group, 2016

Health insurance coverage by age group

Input file: 2016 full-year consolidated (h192)

This is the Python equivalent of R/summary_tables_examples/ins_age_2016.R
"""

import sys
sys.path.insert(0, "../..")

import pandas as pd
import numpy as np

from meps import read_meps, MEPSSurveyDesign, svymean, svytotal, svyby
from meps.utils import recode_factor


def main():
    print("Loading 2016 FYC data...")
    fyc = read_meps(year=2016, type="FYC")

    print("Defining variables...")

    fyc["age_grp"] = pd.cut(
        fyc["AGELAST"],
        bins=[-1, 17, 44, 64, 200],
        labels=["Under 18", "18-44", "45-64", "65+"],
    )

    fyc["insurance"] = recode_factor(
        fyc["INSCOV16"],
        {
            1: "Any private",
            2: "Public only",
            3: "Uninsured",
        },
    )

    print("\nQC: age_grp by AGELAST")
    print(fyc.groupby("age_grp").agg({"AGELAST": ["min", "max", "count"]}))

    print("\nQC: insurance by INSCOV16")
    print(fyc.groupby(["insurance", "INSCOV16"]).size())

    print("\nDefining survey design...")
    design = MEPSSurveyDesign(
        data=fyc,
        id_var="VARPSU",
        strata_var="VARSTR",
        weight_var="PERWT16F",
    )

    print("\n" + "=" * 60)
    print("Health insurance coverage by age group")
    print("=" * 60)

    print("\n--- Number (svytotal) ---")
    result = svyby(design, "insurance", "age_grp", svytotal)
    print(result.to_string(index=False))

    print("\n--- Percent (svymean) ---")
    result = svyby(design, "insurance", "age_grp", svymean)
    print(result.to_string(index=False))


if __name__ == "__main__":
    main()
