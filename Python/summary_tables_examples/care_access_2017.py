"""
Care Access Summary Table: 2017

This program generates the following estimates on care access:
    - Usual source of care
    - Difficulty receiving needed care
    - Difficulty getting appointments

Input file:
    - 2017 Full-year consolidated file (h201)

This is a Python translation of the SAS program care_access_2017.sas
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
    print("MEPS SUMMARY TABLE: CARE ACCESS, 2017")
    print("=" * 70)

    # Load 2017 Full-Year Consolidated file
    fyc_path = "C:/MEPS/h201.sas7bdat"

    print(f"\nLoading data from: {fyc_path}")

    try:
        fyc = load_meps_data(fyc_path)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please download the 2017 FYC file (h201) from the MEPS website:")
        print("https://meps.ahrq.gov/mepsweb/data_stats/download_data_files.jsp")
        return

    # Keep needed variables
    kept_vars = ['VARSTR', 'VARPSU', 'PERWT17F', 'AGELAST', 'SEX',
                 'HAVEUS42', 'LOCATN42', 'MDUNAB42', 'MDUNRS42',
                 'MDDLAY42', 'MDDLRS42', 'APPTDN42', 'APPTRS42']
    available_vars = [v for v in kept_vars if v in fyc.columns]
    data = fyc[available_vars].copy()

    print(f"Records loaded: {len(data):,}")

    # Create age groups
    data['AGECAT'] = pd.cut(
        data['AGELAST'],
        bins=[-1, 17, 44, 64, 200],
        labels=['0-17', '18-44', '45-64', '65+']
    )

    # Define labels
    sex_labels = {1: 'Male', 2: 'Female'}
    haveus_labels = {
        1: 'Yes',
        2: 'No',
        3: 'Don\'t know',
        -1: 'Inapplicable',
        -7: 'Refused',
        -8: 'Don\'t know'
    }

    # Create survey design
    design = MEPSSurveyDesign(data, year=2017)

    # Usual source of care
    print("\n" + "=" * 70)
    print("USUAL SOURCE OF CARE (HAVEUS42)")
    print("=" * 70)

    print(f"\n{'Category':30s} {'N':>10s} {'Population':>15s} {'Percent':>10s}")
    print("-" * 70)

    total_pop = data['PERWT17F'].sum()
    for val, label in haveus_labels.items():
        subset = data[data['HAVEUS42'] == val]
        if len(subset) > 0:
            pop = subset['PERWT17F'].sum()
            pct = pop / total_pop * 100
            print(f"{label:30s} {len(subset):>10,} {pop:>15,.0f} {pct:>10.1f}%")

    # Difficulty receiving needed care
    print("\n" + "=" * 70)
    print("UNABLE TO GET NECESSARY MEDICAL CARE (MDUNAB42)")
    print("=" * 70)

    if 'MDUNAB42' in data.columns:
        mdunab_labels = {1: 'Yes', 2: 'No', -1: 'Inapplicable', -7: 'Refused', -8: 'Don\'t know'}
        print(f"\n{'Category':30s} {'N':>10s} {'Population':>15s} {'Percent':>10s}")
        print("-" * 70)

        for val, label in mdunab_labels.items():
            subset = data[data['MDUNAB42'] == val]
            if len(subset) > 0:
                pop = subset['PERWT17F'].sum()
                pct = pop / total_pop * 100
                print(f"{label:30s} {len(subset):>10,} {pop:>15,.0f} {pct:>10.1f}%")

    # Delayed medical care
    print("\n" + "=" * 70)
    print("DELAYED NECESSARY MEDICAL CARE (MDDLAY42)")
    print("=" * 70)

    if 'MDDLAY42' in data.columns:
        mddlay_labels = {1: 'Yes', 2: 'No', -1: 'Inapplicable', -7: 'Refused', -8: 'Don\'t know'}
        print(f"\n{'Category':30s} {'N':>10s} {'Population':>15s} {'Percent':>10s}")
        print("-" * 70)

        for val, label in mddlay_labels.items():
            subset = data[data['MDDLAY42'] == val]
            if len(subset) > 0:
                pop = subset['PERWT17F'].sum()
                pct = pop / total_pop * 100
                print(f"{label:30s} {len(subset):>10,} {pop:>15,.0f} {pct:>10.1f}%")

    # Difficulty getting appointments
    print("\n" + "=" * 70)
    print("DIFFICULTY GETTING APPOINTMENTS (APPTDN42)")
    print("=" * 70)

    if 'APPTDN42' in data.columns:
        apptdn_labels = {1: 'Yes', 2: 'No', -1: 'Inapplicable', -7: 'Refused', -8: 'Don\'t know'}
        print(f"\n{'Category':30s} {'N':>10s} {'Population':>15s} {'Percent':>10s}")
        print("-" * 70)

        for val, label in apptdn_labels.items():
            subset = data[data['APPTDN42'] == val]
            if len(subset) > 0:
                pop = subset['PERWT17F'].sum()
                pct = pop / total_pop * 100
                print(f"{label:30s} {len(subset):>10,} {pop:>15,.0f} {pct:>10.1f}%")

    # By age group
    print("\n" + "=" * 70)
    print("USUAL SOURCE OF CARE BY AGE GROUP")
    print("=" * 70)

    for age_cat in ['0-17', '18-44', '45-64', '65+']:
        subset = data[data['AGECAT'] == age_cat]
        if len(subset) > 0:
            print(f"\nAge Group: {age_cat}")
            print(f"{'Has USC':30s} {'N':>10s} {'Population':>15s} {'Percent':>10s}")
            print("-" * 70)

            age_pop = subset['PERWT17F'].sum()
            for val in [1, 2]:
                label = 'Yes' if val == 1 else 'No'
                sub2 = subset[subset['HAVEUS42'] == val]
                if len(sub2) > 0:
                    pop = sub2['PERWT17F'].sum()
                    pct = pop / age_pop * 100
                    print(f"{label:30s} {len(sub2):>10,} {pop:>15,.0f} {pct:>10.1f}%")

    print("\n" + "=" * 70)
    print("Analysis complete.")
    print("=" * 70)


if __name__ == "__main__":
    main()
