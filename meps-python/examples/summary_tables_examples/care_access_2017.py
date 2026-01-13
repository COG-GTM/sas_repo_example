"""
Example code to replicate estimates from the MEPS-HC Data Tools summary tables

Accessibility and quality of care: Access to Care, 2017

Reasons for difficulty receiving needed care
 - Number/percent of people
 - By poverty status

Input file: 2017 full-year consolidated (h201)

This is the Python equivalent of R/summary_tables_examples/care_access_2017.R
"""

import sys
sys.path.insert(0, "../..")

import pandas as pd
import numpy as np

from meps import read_meps, MEPSSurveyDesign, svymean, svytotal, svyby
from meps.utils import recode_factor, create_indicator


def main():
    print("Loading 2017 FYC data...")
    fyc = read_meps(year=2017, type="FYC")

    print("Defining variables...")

    fyc["delay_MD"] = ((fyc["MDUNAB42"] == 1) | (fyc["MDDLAY42"] == 1)).astype(int)
    fyc["delay_DN"] = ((fyc["DNUNAB42"] == 1) | (fyc["DNDLAY42"] == 1)).astype(int)
    fyc["delay_PM"] = ((fyc["PMUNAB42"] == 1) | (fyc["PMDLAY42"] == 1)).astype(int)

    fyc["afford_MD"] = ((fyc["MDDLRS42"] == 1) | (fyc["MDUNRS42"] == 1)).astype(int)
    fyc["afford_DN"] = ((fyc["DNDLRS42"] == 1) | (fyc["DNUNRS42"] == 1)).astype(int)
    fyc["afford_PM"] = ((fyc["PMDLRS42"] == 1) | (fyc["PMUNRS42"] == 1)).astype(int)

    fyc["insure_MD"] = (fyc["MDDLRS42"].isin([2, 3]) | fyc["MDUNRS42"].isin([2, 3])).astype(int)
    fyc["insure_DN"] = (fyc["DNDLRS42"].isin([2, 3]) | fyc["DNUNRS42"].isin([2, 3])).astype(int)
    fyc["insure_PM"] = (fyc["PMDLRS42"].isin([2, 3]) | fyc["PMUNRS42"].isin([2, 3])).astype(int)

    fyc["other_MD"] = ((fyc["MDDLRS42"] > 3) | (fyc["MDUNRS42"] > 3)).astype(int)
    fyc["other_DN"] = ((fyc["DNDLRS42"] > 3) | (fyc["DNUNRS42"] > 3)).astype(int)
    fyc["other_PM"] = ((fyc["PMDLRS42"] > 3) | (fyc["PMUNRS42"] > 3)).astype(int)

    fyc["delay_ANY"] = (fyc["delay_MD"] | fyc["delay_DN"] | fyc["delay_PM"]).astype(int)
    fyc["afford_ANY"] = (fyc["afford_MD"] | fyc["afford_DN"] | fyc["afford_PM"]).astype(int)
    fyc["insure_ANY"] = (fyc["insure_MD"] | fyc["insure_DN"] | fyc["insure_PM"]).astype(int)
    fyc["other_ANY"] = (fyc["other_MD"] | fyc["other_DN"] | fyc["other_PM"]).astype(int)

    fyc["poverty"] = recode_factor(
        fyc["POVCAT17"],
        {
            1: "Negative or poor",
            2: "Near-poor",
            3: "Low income",
            4: "Middle income",
            5: "High income",
        },
    )

    print("\nQC: delay_MD by MDUNAB42 and MDDLAY42")
    print(fyc.groupby(["delay_MD", "MDUNAB42", "MDDLAY42"]).size())

    print("\nQC: poverty by POVCAT17")
    print(fyc.groupby(["poverty", "POVCAT17"]).size())

    print("\nDefining survey design...")
    design = MEPSSurveyDesign(
        data=fyc,
        id_var="VARPSU",
        strata_var="VARSTR",
        weight_var="PERWT17F",
    )

    print("\nSubsetting to persons eligible for access to care supplement who experienced difficulty...")
    sub_condition = (fyc["ACCELI42"] == 1) & (fyc["delay_ANY"] == 1)
    sub_design = design.subset(sub_condition)

    print("\n" + "=" * 60)
    print("Reasons for difficulty receiving any needed care, by poverty status")
    print("=" * 60)

    print("\n--- Number (svytotal) ---")
    for reason in ["afford_ANY", "insure_ANY", "other_ANY"]:
        print(f"\n{reason}:")
        result = svyby(sub_design, reason, "poverty", svytotal)
        print(result.to_string(index=False))

    print("\n--- Percent (svymean) ---")
    for reason in ["afford_ANY", "insure_ANY", "other_ANY"]:
        print(f"\n{reason}:")
        result = svyby(sub_design, reason, "poverty", svymean)
        print(result.to_string(index=False))


if __name__ == "__main__":
    main()
