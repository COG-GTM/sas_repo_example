"""
Exercise 6a: Logistic Regression for Flu Shot, 2018

This program includes a regression example for persons receiving a flu shot
in the last 12 months for the civilian noninstitutionalized population:
    - Percentage of people with a flu shot (civilian noninstitutionalized population), 2018
    - Logistic regression: to identify demographic factors associated with receiving a flu shot

Input file:
    - 2018 Full-year consolidated file (h209)

This is a Python translation of the SAS program Exercise6.sas (exercise_6a)
"""

import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from utils.meps_loader import load_meps_data
from utils.survey_design import MEPSSurveyDesign


def main():
    print("=" * 70)
    print("MEPS DATA USERS WORKSHOP")
    print("EXERCISE 6a: LOGISTIC REGRESSION FOR FLU SHOT, 2018")
    print("=" * 70)

    # Load 2018 Full-Year Consolidated file
    fyc_path = "C:/MEPS/h209.sas7bdat"

    print(f"\nLoading data from: {fyc_path}")

    try:
        fyc = load_meps_data(fyc_path)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please download the 2018 FYC file (h209) from the MEPS website:")
        print("https://meps.ahrq.gov/mepsweb/data_stats/download_data_files.jsp")
        return

    # Keep only needed variables
    kept_vars = ['VARSTR', 'VARPSU', 'PERWT18F', 'SAQWT18F', 'ADFLST42',
                 'AGELAST', 'RACETHX', 'POVCAT18', 'INSCOV18', 'SEX']
    available_vars = [v for v in kept_vars if v in fyc.columns]
    data = fyc[available_vars].copy()

    print(f"Records loaded: {len(data):,}")

    # Create flu shot variable
    # ADFLST42: 1 = Yes, 2 = No, -1 = Inapplicable, -7/-8 = DK/Refused, -15 = Cannot be computed
    print("\n" + "-" * 70)
    print("Step 1: Create flu shot indicator variable")
    print("-" * 70)

    data['FLUSHOT'] = np.where(
        data['ADFLST42'] == 1, 1,
        np.where(data['ADFLST42'] == 2, 0, np.nan)
    )

    print(f"\nFlu shot distribution (ADFLST42):")
    print(data['ADFLST42'].value_counts(dropna=False).sort_index())

    print(f"\nFlu shot indicator (FLUSHOT):")
    print(data['FLUSHOT'].value_counts(dropna=False))

    # Create subpopulation indicator (adults 18+)
    data['ADULT'] = (data['AGELAST'] >= 18).astype(int)

    print(f"\nAdults 18+ (ADULT=1): {(data['ADULT'] == 1).sum():,}")

    # Define labels for categorical variables
    sex_labels = {1: 'Male', 2: 'Female'}
    racethx_labels = {
        1: 'Hispanic',
        2: 'NH White only',
        3: 'NH Black only',
        4: 'NH Asian only',
        5: 'NH Other etc'
    }
    inscov_labels = {
        1: 'Any Private',
        2: 'Public Only',
        3: 'Uninsured'
    }

    # Calculate percentage with flu shot among adults
    print("\n" + "=" * 70)
    print("PERCENTAGE OF ADULTS 18+ WITH FLU SHOT, 2018")
    print("=" * 70)

    # Filter to adults with valid flu shot data and SAQ weight
    adults = data[(data['ADULT'] == 1) &
                  (data['FLUSHOT'].notna()) &
                  (data['SAQWT18F'] > 0)].copy()

    print(f"\nAdults with valid flu shot data: {len(adults):,}")

    # Create survey design using SAQ weights
    design = MEPSSurveyDesign(adults, year=2018, weights='SAQWT18F')

    # Calculate proportion with flu shot
    flu_results = design.mean('FLUSHOT')
    print(f"\nPercentage with flu shot:")
    print(f"  N:          {flu_results['N'].values[0]:,.0f}")
    print(f"  Population: {flu_results['SumWgt'].values[0]:,.0f}")
    print(f"  Proportion: {flu_results['Mean'].values[0]:.4f} ({flu_results['Mean'].values[0]*100:.1f}%)")
    print(f"  SE:         {flu_results['SE'].values[0]:.5f}")

    # Logistic regression
    print("\n" + "=" * 70)
    print("LOGISTIC REGRESSION: FACTORS ASSOCIATED WITH FLU SHOT")
    print("=" * 70)

    # Prepare data for regression
    # Create dummy variables for categorical predictors
    reg_data = adults.copy()

    # Sex (reference = Male)
    reg_data['FEMALE'] = (reg_data['SEX'] == 2).astype(int)

    # Race/Ethnicity (reference = Hispanic)
    reg_data['NH_WHITE'] = (reg_data['RACETHX'] == 2).astype(int)
    reg_data['NH_BLACK'] = (reg_data['RACETHX'] == 3).astype(int)
    reg_data['NH_ASIAN'] = (reg_data['RACETHX'] == 4).astype(int)
    reg_data['NH_OTHER'] = (reg_data['RACETHX'] == 5).astype(int)

    # Insurance coverage (reference = Any Private)
    reg_data['PUBLIC_ONLY'] = (reg_data['INSCOV18'] == 2).astype(int)
    reg_data['UNINSURED'] = (reg_data['INSCOV18'] == 3).astype(int)

    # Filter to complete cases
    reg_vars = ['FLUSHOT', 'AGELAST', 'FEMALE', 'NH_WHITE', 'NH_BLACK',
                'NH_ASIAN', 'NH_OTHER', 'PUBLIC_ONLY', 'UNINSURED',
                'VARSTR', 'VARPSU', 'SAQWT18F']
    reg_data = reg_data[reg_vars].dropna()

    print(f"\nRecords for regression: {len(reg_data):,}")

    # Create survey design for regression
    design_reg = MEPSSurveyDesign(reg_data, year=2018, weights='SAQWT18F')

    # Fit logistic regression
    formula = ('FLUSHOT ~ AGELAST + FEMALE + NH_WHITE + NH_BLACK + '
               'NH_ASIAN + NH_OTHER + PUBLIC_ONLY + UNINSURED')

    print(f"\nModel formula: {formula}")
    print("\nReference categories:")
    print("  Sex: Male")
    print("  Race/Ethnicity: Hispanic")
    print("  Insurance: Any Private")

    results = design_reg.glm(formula, family='binomial')

    if 'error' in results:
        print(f"\nError fitting model: {results['error']}")
        print("\nNote: For full survey-weighted logistic regression,")
        print("consider using statsmodels with proper survey weights.")
    else:
        print("\n" + "-" * 70)
        print("Logistic Regression Results")
        print("-" * 70)

        # Print coefficients
        print(f"\n{'Variable':20s} {'Coef':>12s} {'SE':>12s} {'P-value':>12s}")
        print("-" * 60)

        for var in results['params'].index:
            coef = results['params'][var]
            se = results['bse'][var]
            pval = results['pvalues'][var]
            print(f"{var:20s} {coef:>12.4f} {se:>12.4f} {pval:>12.4f}")

        # Print odds ratios
        print("\n" + "-" * 70)
        print("Odds Ratios")
        print("-" * 70)

        print(f"\n{'Variable':20s} {'Odds Ratio':>12s} {'95% CI Lower':>14s} {'95% CI Upper':>14s}")
        print("-" * 65)

        conf_int = results['conf_int']
        for var in results['params'].index:
            if var != 'Intercept':
                odds_ratio = np.exp(results['params'][var])
                ci_lower = np.exp(conf_int.loc[var, 0])
                ci_upper = np.exp(conf_int.loc[var, 1])
                print(f"{var:20s} {odds_ratio:>12.4f} {ci_lower:>14.4f} {ci_upper:>14.4f}")

        print(f"\nNumber of observations: {results['nobs']:.0f}")
        print(f"AIC: {results['aic']:.2f}")

    print("\n" + "=" * 70)
    print("Analysis complete.")
    print("=" * 70)
    print("\nNote: This analysis uses SAQ weights (SAQWT18F) because the")
    print("flu shot question (ADFLST42) is from the Self-Administered")
    print("Questionnaire, which is administered to a subset of the sample.")


if __name__ == "__main__":
    main()
