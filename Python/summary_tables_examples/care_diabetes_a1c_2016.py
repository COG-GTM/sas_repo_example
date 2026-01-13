"""
Diabetes Care Summary Table: A1c Testing, 2016

This program generates estimates on diabetes care:
    - Percentage of persons with diabetes who had A1c testing
    - By demographic characteristics (age, sex, race/ethnicity)

Input file:
    - 2016 Full-year consolidated file (h192)

This is a Python translation of the SAS program care_diabetes_a1c_2016.sas
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
    print("MEPS SUMMARY TABLE: DIABETES CARE - A1c TESTING, 2016")
    print("=" * 70)

    # Load 2016 Full-Year Consolidated file
    fyc_path = "C:/MEPS/h192.sas7bdat"

    print(f"\nLoading data from: {fyc_path}")

    try:
        fyc = load_meps_data(fyc_path)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please download the 2016 FYC file (h192) from the MEPS website:")
        print("https://meps.ahrq.gov/mepsweb/data_stats/download_data_files.jsp")
        return

    # Keep needed variables
    kept_vars = ['VARSTR', 'VARPSU', 'PERWT16F', 'SAQWT16F', 'AGELAST', 'SEX',
                 'RACETHX', 'DIABDX', 'DSA1C53', 'DSCKNO53']
    available_vars = [v for v in kept_vars if v in fyc.columns]
    data = fyc[available_vars].copy()

    print(f"Records loaded: {len(data):,}")

    # Filter to persons with diabetes
    # DIABDX = 1 (Yes, diagnosed with diabetes)
    data_diab = data[data['DIABDX'] == 1].copy()
    print(f"Persons with diabetes: {len(data_diab):,}")

    # Create A1c testing indicator
    # DSA1C53: 1 = Yes, 2 = No, -1 = Inapplicable, -7/-8 = DK/Refused
    data_diab['A1C_TEST'] = np.where(
        data_diab['DSA1C53'] == 1, 1,
        np.where(data_diab['DSA1C53'] == 2, 0, np.nan)
    )

    # Filter to valid responses
    data_valid = data_diab[data_diab['A1C_TEST'].notna()].copy()
    print(f"Persons with valid A1c response: {len(data_valid):,}")

    # Define labels
    sex_labels = {1: 'Male', 2: 'Female'}
    racethx_labels = {
        1: 'Hispanic',
        2: 'NH White only',
        3: 'NH Black only',
        4: 'NH Asian only',
        5: 'NH Other etc'
    }

    # Create age groups
    data_valid['AGECAT'] = pd.cut(
        data_valid['AGELAST'],
        bins=[-1, 44, 64, 200],
        labels=['18-44', '45-64', '65+']
    )

    # Create survey design using SAQ weights
    design = MEPSSurveyDesign(data_valid, year=2016, weights='SAQWT16F')

    # Overall A1c testing rate
    print("\n" + "=" * 70)
    print("A1c TESTING AMONG PERSONS WITH DIABETES")
    print("=" * 70)

    a1c_results = design.mean('A1C_TEST')
    print(f"\nOverall A1c testing rate:")
    print(f"  N:          {a1c_results['N'].values[0]:,.0f}")
    print(f"  Population: {a1c_results['SumWgt'].values[0]:,.0f}")
    print(f"  Proportion: {a1c_results['Mean'].values[0]:.4f} ({a1c_results['Mean'].values[0]*100:.1f}%)")
    print(f"  SE:         {a1c_results['SE'].values[0]:.5f}")

    # By sex
    print("\n" + "-" * 70)
    print("A1c Testing Rate by Sex")
    print("-" * 70)

    print(f"\n{'Sex':20s} {'N':>10s} {'Percent':>12s} {'SE':>10s}")
    print("-" * 55)

    for sex_val, sex_label in sex_labels.items():
        subset = data_valid[data_valid['SEX'] == sex_val].copy()
        if len(subset) > 0:
            design_sub = MEPSSurveyDesign(subset, year=2016, weights='SAQWT16F')
            results = design_sub.mean('A1C_TEST')
            print(f"{sex_label:20s} {results['N'].values[0]:>10,.0f} "
                  f"{results['Mean'].values[0]*100:>11.1f}% {results['SE'].values[0]*100:>9.2f}%")

    # By race/ethnicity
    print("\n" + "-" * 70)
    print("A1c Testing Rate by Race/Ethnicity")
    print("-" * 70)

    print(f"\n{'Race/Ethnicity':20s} {'N':>10s} {'Percent':>12s} {'SE':>10s}")
    print("-" * 55)

    for race_val, race_label in racethx_labels.items():
        subset = data_valid[data_valid['RACETHX'] == race_val].copy()
        if len(subset) > 0:
            design_sub = MEPSSurveyDesign(subset, year=2016, weights='SAQWT16F')
            results = design_sub.mean('A1C_TEST')
            print(f"{race_label:20s} {results['N'].values[0]:>10,.0f} "
                  f"{results['Mean'].values[0]*100:>11.1f}% {results['SE'].values[0]*100:>9.2f}%")

    # By age group
    print("\n" + "-" * 70)
    print("A1c Testing Rate by Age Group")
    print("-" * 70)

    print(f"\n{'Age Group':20s} {'N':>10s} {'Percent':>12s} {'SE':>10s}")
    print("-" * 55)

    for age_cat in ['18-44', '45-64', '65+']:
        subset = data_valid[data_valid['AGECAT'] == age_cat].copy()
        if len(subset) > 0:
            design_sub = MEPSSurveyDesign(subset, year=2016, weights='SAQWT16F')
            results = design_sub.mean('A1C_TEST')
            print(f"{age_cat:20s} {results['N'].values[0]:>10,.0f} "
                  f"{results['Mean'].values[0]*100:>11.1f}% {results['SE'].values[0]*100:>9.2f}%")

    # Number of A1c checks
    print("\n" + "=" * 70)
    print("NUMBER OF A1c CHECKS IN PAST YEAR (DSCKNO53)")
    print("=" * 70)

    if 'DSCKNO53' in data_valid.columns:
        # Filter to those who had A1c test
        data_a1c = data_valid[data_valid['A1C_TEST'] == 1].copy()
        data_a1c = data_a1c[data_a1c['DSCKNO53'] > 0].copy()

        if len(data_a1c) > 0:
            design_a1c = MEPSSurveyDesign(data_a1c, year=2016, weights='SAQWT16F')
            checks_mean = design_a1c.mean('DSCKNO53')
            print(f"\nMean number of A1c checks (among those tested):")
            print(f"  N:    {checks_mean['N'].values[0]:,.0f}")
            print(f"  Mean: {checks_mean['Mean'].values[0]:.2f}")
            print(f"  SE:   {checks_mean['SE'].values[0]:.4f}")

    print("\n" + "=" * 70)
    print("Analysis complete.")
    print("=" * 70)
    print("\nNote: This analysis uses SAQ weights (SAQWT16F) because the")
    print("diabetes care questions are from the Self-Administered Questionnaire.")


if __name__ == "__main__":
    main()
