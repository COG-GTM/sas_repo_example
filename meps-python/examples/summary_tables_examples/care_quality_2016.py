"""
Example code to replicate estimates from the MEPS-HC Data Tools summary tables

Accessibility and quality of care: Quality of Care, 2016

Self-administered questionnaire (SAQ):
 - Number/percent of adults by ability to schedule a routine appointment
 - By insurance coverage status

Input file: 2016 full-year consolidated (h192)

This is the Python equivalent of R/summary_tables_examples/care_quality_2016.R

Note: Uses SAQWT16F weight variable since outcome variable comes from SAQ
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

    fyc["adult_routine"] = recode_factor(
        fyc["ADRTWW42"],
        {
            4: "Always",
            3: "Usually",
            2: "Sometimes/Never",
            1: "Sometimes/Never",
            -7: "Don't know/Non-response",
            -8: "Don't know/Non-response",
            -9: "Don't know/Non-response",
            -1: "Inapplicable",
        },
        default="Missing",
        missing="Missing",
    )

    fyc["insurance"] = recode_factor(
        fyc["INSURC16"],
        {
            1: "<65, Any private",
            2: "<65, Public only",
            3: "<65, Uninsured",
            4: "65+, Medicare only",
            5: "65+, Medicare and private",
            6: "65+, Medicare and other public",
            7: "65+, No medicare",
            8: "65+, No medicare",
        },
        default="Missing",
        missing="Missing",
    )

    print("\nQC: adult_routine by ADRTWW42")
    print(fyc.groupby(["adult_routine", "ADRTWW42"]).size().head(20))

    print("\nQC: insurance by INSURC16")
    print(fyc.groupby(["insurance", "INSURC16"]).size())

    print("\nDefining survey design with SAQ weight...")
    saq_design = MEPSSurveyDesign(
        data=fyc,
        id_var="VARPSU",
        strata_var="VARSTR",
        weight_var="SAQWT16F",
    )

    print("\nSubsetting to adults who made an appointment...")
    sub_condition = (fyc["ADRTCR42"] == 1) & (fyc["AGELAST"] >= 18)
    sub_design = saq_design.subset(sub_condition)

    print("\n" + "=" * 60)
    print("Ability to schedule a routine appointment (adults), by insurance coverage")
    print("=" * 60)

    print("\n--- Number (svytotal) ---")
    result_total = svyby(sub_design, "adult_routine", "insurance", svytotal)
    print(result_total.to_string(index=False))

    print("\n--- Percent (svymean) ---")
    result_mean = svyby(sub_design, "adult_routine", "insurance", svymean)
    print(result_mean.to_string(index=False))


if __name__ == "__main__":
    main()
