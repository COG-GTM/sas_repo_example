"""
Health Insurance Summary Table: Coverage by Age, 2016

This program generates estimates on health insurance coverage:
    - Insurance coverage type by age group
    - Percentage with any private, public only, uninsured

Input file:
    - 2016 Full-year consolidated file (h192)

This is a Python translation of the Stata program ins_age_2016.do
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
    print("MEPS SUMMARY TABLE: INSURANCE COVERAGE BY AGE, 2016")
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
    kept_vars = ['VARSTR', 'VARPSU', 'PERWT16F', 'AGELAST', 'SEX',
                 'INSCOV16', 'MCDEV16', 'MCARE16', 'PRVEV16', 'UNINS16']
    available_vars = [v for v in kept_vars if v in fyc.columns]
    data = fyc[available_vars].copy()

    print(f"Records loaded: {len(data):,}")

    # Create age groups
    data['AGECAT'] = pd.cut(
        data['AGELAST'],
        bins=[-1, 17, 44, 64, 200],
        labels=['0-17', '18-44', '45-64', '65+']
    )

    # Define insurance coverage labels
    inscov_labels = {
        1: 'Any Private',
        2: 'Public Only',
        3: 'Uninsured'
    }

    # Create insurance type indicators
    data['ANY_PRIVATE'] = (data['INSCOV16'] == 1).astype(int)
    data['PUBLIC_ONLY'] = (data['INSCOV16'] == 2).astype(int)
    data['UNINSURED'] = (data['INSCOV16'] == 3).astype(int)

    # Create survey design
    design = MEPSSurveyDesign(data, year=2016)

    # Overall insurance coverage
    print("\n" + "=" * 70)
    print("OVERALL INSURANCE COVERAGE")
    print("=" * 70)

    print(f"\n{'Coverage Type':20s} {'N':>10s} {'Population':>15s} {'Percent':>10s} {'SE':>10s}")
    print("-" * 70)

    total_pop = data['PERWT16F'].sum()

    for inscov_val, inscov_label in inscov_labels.items():
        subset = data[data['INSCOV16'] == inscov_val]
        if len(subset) > 0:
            pop = subset['PERWT16F'].sum()
            pct = pop / total_pop * 100
            
            # Calculate SE using survey design
            var_name = ['ANY_PRIVATE', 'PUBLIC_ONLY', 'UNINSURED'][inscov_val - 1]
            results = design.mean(var_name)
            se = results['SE'].values[0] * 100
            
            print(f"{inscov_label:20s} {len(subset):>10,} {pop:>15,.0f} {pct:>9.1f}% {se:>9.2f}%")

    # Insurance coverage by age group
    print("\n" + "=" * 70)
    print("INSURANCE COVERAGE BY AGE GROUP")
    print("=" * 70)

    for age_cat in ['0-17', '18-44', '45-64', '65+']:
        subset = data[data['AGECAT'] == age_cat]
        if len(subset) > 0:
            print(f"\nAge Group: {age_cat}")
            print(f"{'Coverage Type':20s} {'N':>10s} {'Population':>15s} {'Percent':>10s}")
            print("-" * 60)

            age_pop = subset['PERWT16F'].sum()
            for inscov_val, inscov_label in inscov_labels.items():
                sub2 = subset[subset['INSCOV16'] == inscov_val]
                if len(sub2) > 0:
                    pop = sub2['PERWT16F'].sum()
                    pct = pop / age_pop * 100
                    print(f"{inscov_label:20s} {len(sub2):>10,} {pop:>15,.0f} {pct:>9.1f}%")

    # Detailed coverage types
    print("\n" + "=" * 70)
    print("DETAILED COVERAGE TYPES")
    print("=" * 70)

    # Medicare coverage
    if 'MCARE16' in data.columns:
        data['HAS_MEDICARE'] = (data['MCARE16'] == 1).astype(int)
        medicare_results = design.mean('HAS_MEDICARE')
        print(f"\nMedicare coverage:")
        print(f"  Percent: {medicare_results['Mean'].values[0]*100:.1f}%")
        print(f"  SE:      {medicare_results['SE'].values[0]*100:.2f}%")

    # Medicaid coverage
    if 'MCDEV16' in data.columns:
        data['HAS_MEDICAID'] = (data['MCDEV16'] == 1).astype(int)
        medicaid_results = design.mean('HAS_MEDICAID')
        print(f"\nMedicaid coverage:")
        print(f"  Percent: {medicaid_results['Mean'].values[0]*100:.1f}%")
        print(f"  SE:      {medicaid_results['SE'].values[0]*100:.2f}%")

    # Private coverage
    if 'PRVEV16' in data.columns:
        data['HAS_PRIVATE'] = (data['PRVEV16'] == 1).astype(int)
        private_results = design.mean('HAS_PRIVATE')
        print(f"\nPrivate coverage:")
        print(f"  Percent: {private_results['Mean'].values[0]*100:.1f}%")
        print(f"  SE:      {private_results['SE'].values[0]*100:.2f}%")

    # Uninsured all year
    if 'UNINS16' in data.columns:
        data['UNINS_ALL_YEAR'] = (data['UNINS16'] == 1).astype(int)
        unins_results = design.mean('UNINS_ALL_YEAR')
        print(f"\nUninsured all year:")
        print(f"  Percent: {unins_results['Mean'].values[0]*100:.1f}%")
        print(f"  SE:      {unins_results['SE'].values[0]*100:.2f}%")

    print("\n" + "=" * 70)
    print("Analysis complete.")
    print("=" * 70)


if __name__ == "__main__":
    main()
