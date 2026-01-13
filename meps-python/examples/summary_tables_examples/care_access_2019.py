"""
Example code to replicate estimates from the MEPS-HC Data Tools summary tables

Accessibility and quality of care: Access to Care, 2019

Number and percent of people who did not receive treatment because they couldn't afford it
 - By poverty status

Input file: 2019 full-year consolidated (h216)

This is the Python equivalent of R/summary_tables_examples/care_access_2019.R
"""

import sys
sys.path.insert(0, "../..")

import pandas as pd
import numpy as np

from meps import read_meps, MEPSSurveyDesign, svymean, svytotal, svyby
from meps.utils import recode_factor


def main():
    print("Loading 2019 FYC data...")
    fyc = read_meps(year=2019, type="FYC")

    print("Defining variables...")

    fyc["afford_MD"] = ((fyc["AFRDCA42"] == 1)).astype(int)
    fyc["afford_DN"] = ((fyc["AFRDDN42"] == 1)).astype(int)
    fyc["afford_PM"] = ((fyc["AFRDPM42"] == 1)).astype(int)

    fyc["afford_ANY"] = (fyc["afford_MD"] | fyc["afford_DN"] | fyc["afford_PM"]).astype(int)

    fyc["poverty"] = recode_factor(
        fyc["POVCAT19"],
        {
            1: "Negative or poor",
            2: "Near-poor",
            3: "Low income",
            4: "Middle income",
            5: "High income",
        },
    )

    print("\nQC: afford_ANY by component variables")
    print(fyc.groupby(["afford_ANY", "afford_MD", "afford_DN", "afford_PM"]).size().head(10))

    print("\nQC: poverty by POVCAT19")
    print(fyc.groupby(["poverty", "POVCAT19"]).size())

    print("\nDefining survey design...")
    design = MEPSSurveyDesign(
        data=fyc,
        id_var="VARPSU",
        strata_var="VARSTR",
        weight_var="PERWT19F",
    )

    print("\n" + "=" * 60)
    print("Number and percent who couldn't afford treatment, by poverty status")
    print("=" * 60)

    print("\n--- Number (svytotal) ---")
    result = svyby(design, "afford_ANY", "poverty", svytotal)
    print(result.to_string(index=False))

    print("\n--- Percent (svymean) ---")
    result = svyby(design, "afford_ANY", "poverty", svymean)
    print(result.to_string(index=False))


if __name__ == "__main__":
    main()
