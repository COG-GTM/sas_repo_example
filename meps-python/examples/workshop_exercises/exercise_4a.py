"""
This program includes a regression example for persons receiving a flu shot
in the last 12 months for the U.S. civilian non-institutionalized population,
including:
 - Percentage of people with a flu shot
 - Logistic regression: to identify demographic factors associated with
   receiving a flu shot

Input file:
 - 2018 Full-year file (h209)

This is the Python equivalent of R/workshop_exercises/exercise_4a.R
"""

import sys
sys.path.insert(0, "../..")

import pandas as pd
import numpy as np

from meps import read_meps, MEPSSurveyDesign, svymean, svyglm


def main():
    print("Loading 2018 FYC data...")
    fyc18 = read_meps(year=2018, type="FYC")

    print("\nViewing key variables...")
    print(fyc18[["DUPERSID", "ADFLST42", "AGELAST", "SEX", "RACETHX", "INSCOV18"]].head(10))

    print("\nADFLST42 distribution (among SAQ respondents):")
    print(fyc18[fyc18["SAQWT18F"] > 0].groupby("ADFLST42").size())

    print("\nKeeping only needed variables...")
    fyc18_sub = fyc18[["DUPERSID", "VARPSU", "VARSTR", "ADFLST42", "AGELAST",
                       "SEX", "RACETHX", "INSCOV18", "SAQWT18F"]].copy()

    print("\nCreating variables...")
    conditions = [
        fyc18_sub["ADFLST42"] == 1,
        fyc18_sub["ADFLST42"] == 2,
    ]
    choices = [1, 0]
    fyc18_sub["flu_shot"] = np.select(conditions, choices, default=fyc18_sub["ADFLST42"])

    fyc18_sub["subpop"] = (fyc18_sub["ADFLST42"] >= 0).astype(int)

    print("\nQC: flu_shot by ADFLST42 and subpop")
    print(fyc18_sub.groupby(["flu_shot", "ADFLST42", "subpop"]).size())

    print("\nChecking regression variables...")
    print("\nSEX:")
    print("  1 = MALE")
    print("  2 = FEMALE")
    print(fyc18_sub.groupby("SEX").size())

    print("\nRACETHX:")
    print("  1 = HISPANIC")
    print("  2 = NON-HISPANIC WHITE")
    print("  3 = NON-HISPANIC BLACK")
    print("  4 = NON-HISPANIC ASIAN")
    print("  5 = NON-HISPANIC OTHER/MULTIPLE")
    print(fyc18_sub.groupby("RACETHX").size())

    print("\nINSCOV18:")
    print("  1 = ANY PRIVATE")
    print("  2 = PUBLIC ONLY")
    print("  3 = UNINSURED")
    print(fyc18_sub.groupby("INSCOV18").size())

    print("\nAGELAST range:")
    print(f"  Min: {fyc18_sub['AGELAST'].min()}, Max: {fyc18_sub['AGELAST'].max()}")

    print("\nDefining survey design with SAQ weight...")
    saq_design = MEPSSurveyDesign(
        data=fyc18_sub,
        id_var="VARPSU",
        strata_var="VARSTR",
        weight_var="SAQWT18F",
    )

    flu_design = saq_design.subset(fyc18_sub["subpop"] == 1)

    print("\nQC: flu_shot distribution in design")
    print(flu_design.data.groupby("flu_shot").size())

    print("\n" + "=" * 70)
    print("SURVEY ESTIMATES")
    print("=" * 70)

    print("\nPercentage of people with a flu shot:")
    result = svymean(flu_design, "flu_shot")
    print(result.to_string(index=False))

    print("\nLogistic regression: factors associated with receiving a flu shot")
    print("  Model: flu_shot ~ AGELAST + SEX + RACETHX + INSCOV18")

    result = svyglm(
        flu_design,
        formula="flu_shot ~ AGELAST + as.factor(SEX) + as.factor(RACETHX) + as.factor(INSCOV18)",
        family="quasibinomial",
    )

    if "error" not in result or not result.get("error"):
        print("\nCoefficients:")
        for var, coef in result["coefficients"].items():
            se = result["std_errors"].get(var, np.nan)
            pval = result["p_values"].get(var, np.nan)
            print(f"  {var}: {coef:.6f} (SE: {se:.6f}, p: {pval:.4f})")
    else:
        print(f"\nError in regression: {result.get('error')}")


if __name__ == "__main__":
    main()
