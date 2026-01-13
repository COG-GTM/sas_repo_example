"""
Care Quality Summary Table: Routine Appointment Scheduling, 2016

This program generates estimates on care quality:
    - Ability to get routine appointments
    - Wait time for appointments
    - Satisfaction with care

Input file:
    - 2016 Full-year consolidated file (h192)

This is a Python translation of the R program care_quality_2016.R
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
    print("MEPS SUMMARY TABLE: CARE QUALITY, 2016")
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
                 'HAVEUS42', 'LOCATN42', 'YNOUSC42', 'NOREAS42',
                 'APTS4253', 'APPT4253', 'APTL4253']
    available_vars = [v for v in kept_vars if v in fyc.columns]
    data = fyc[available_vars].copy()

    print(f"Records loaded: {len(data):,}")

    # Filter to adults with SAQ weight
    data_adults = data[(data['AGELAST'] >= 18) & (data['SAQWT16F'] > 0)].copy()
    print(f"Adults with SAQ weight: {len(data_adults):,}")

    # Define labels
    apts_labels = {
        1: 'Always',
        2: 'Usually',
        3: 'Sometimes',
        4: 'Never',
        -1: 'Inapplicable',
        -7: 'Refused',
        -8: 'Don\'t know',
        -9: 'Not ascertained'
    }

    # Create survey design using SAQ weights
    design = MEPSSurveyDesign(data_adults, year=2016, weights='SAQWT16F')

    # Ability to get routine appointments (APTS4253)
    print("\n" + "=" * 70)
    print("ABILITY TO GET ROUTINE APPOINTMENTS (APTS4253)")
    print("=" * 70)

    if 'APTS4253' in data_adults.columns:
        print(f"\n{'Response':30s} {'N':>10s} {'Population':>15s} {'Percent':>10s}")
        print("-" * 70)

        # Filter to valid responses
        valid_data = data_adults[data_adults['APTS4253'].isin([1, 2, 3, 4])].copy()
        total_pop = valid_data['SAQWT16F'].sum()

        for val in [1, 2, 3, 4]:
            label = apts_labels.get(val, str(val))
            subset = valid_data[valid_data['APTS4253'] == val]
            if len(subset) > 0:
                pop = subset['SAQWT16F'].sum()
                pct = pop / total_pop * 100
                print(f"{label:30s} {len(subset):>10,} {pop:>15,.0f} {pct:>9.1f}%")

        # Create indicator for always/usually
        valid_data['GOOD_ACCESS'] = (valid_data['APTS4253'].isin([1, 2])).astype(int)
        design_valid = MEPSSurveyDesign(valid_data, year=2016, weights='SAQWT16F')
        access_results = design_valid.mean('GOOD_ACCESS')
        print(f"\nAlways/Usually get appointments: {access_results['Mean'].values[0]*100:.1f}% "
              f"(SE: {access_results['SE'].values[0]*100:.2f}%)")

    # Wait time for appointments (APPT4253)
    print("\n" + "=" * 70)
    print("WAIT TIME FOR ROUTINE APPOINTMENTS (APPT4253)")
    print("=" * 70)

    appt_labels = {
        1: 'Same day',
        2: '1 day',
        3: '2-3 days',
        4: '4-7 days',
        5: '8-14 days',
        6: '15+ days',
        -1: 'Inapplicable',
        -7: 'Refused',
        -8: 'Don\'t know',
        -9: 'Not ascertained'
    }

    if 'APPT4253' in data_adults.columns:
        print(f"\n{'Wait Time':30s} {'N':>10s} {'Population':>15s} {'Percent':>10s}")
        print("-" * 70)

        # Filter to valid responses
        valid_data = data_adults[data_adults['APPT4253'].isin([1, 2, 3, 4, 5, 6])].copy()
        total_pop = valid_data['SAQWT16F'].sum()

        for val in [1, 2, 3, 4, 5, 6]:
            label = appt_labels.get(val, str(val))
            subset = valid_data[valid_data['APPT4253'] == val]
            if len(subset) > 0:
                pop = subset['SAQWT16F'].sum()
                pct = pop / total_pop * 100
                print(f"{label:30s} {len(subset):>10,} {pop:>15,.0f} {pct:>9.1f}%")

        # Create indicator for within 1 week
        valid_data['QUICK_APPT'] = (valid_data['APPT4253'].isin([1, 2, 3, 4])).astype(int)
        design_valid = MEPSSurveyDesign(valid_data, year=2016, weights='SAQWT16F')
        quick_results = design_valid.mean('QUICK_APPT')
        print(f"\nAppointment within 1 week: {quick_results['Mean'].values[0]*100:.1f}% "
              f"(SE: {quick_results['SE'].values[0]*100:.2f}%)")

    # Usual source of care
    print("\n" + "=" * 70)
    print("USUAL SOURCE OF CARE (HAVEUS42)")
    print("=" * 70)

    if 'HAVEUS42' in data_adults.columns:
        haveus_labels = {1: 'Yes', 2: 'No', 3: 'Don\'t know'}
        print(f"\n{'Has USC':30s} {'N':>10s} {'Population':>15s} {'Percent':>10s}")
        print("-" * 70)

        valid_data = data_adults[data_adults['HAVEUS42'].isin([1, 2])].copy()
        total_pop = valid_data['PERWT16F'].sum()

        for val in [1, 2]:
            label = haveus_labels.get(val, str(val))
            subset = valid_data[valid_data['HAVEUS42'] == val]
            if len(subset) > 0:
                pop = subset['PERWT16F'].sum()
                pct = pop / total_pop * 100
                print(f"{label:30s} {len(subset):>10,} {pop:>15,.0f} {pct:>9.1f}%")

    print("\n" + "=" * 70)
    print("Analysis complete.")
    print("=" * 70)
    print("\nNote: This analysis uses SAQ weights (SAQWT16F) for questions")
    print("from the Self-Administered Questionnaire.")


if __name__ == "__main__":
    main()
