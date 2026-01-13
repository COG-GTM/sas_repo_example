"""
Exercise 3a: Use and Expenditures for Persons with Diabetes, 2015

This program illustrates how to identify persons with a condition and
calculate estimates on use and expenditures for persons with the condition.

The condition used in this exercise is Diabetes (CCS CODE=049 or 050)

Input files:
    (1) C:/MEPS/h180.sas7bdat (2015 Condition PUF)
    (2) C:/MEPS/h181.sas7bdat (2015 Full-Year PUF)

This is a Python translation of the SAS program Exercise3a.sas
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
    print("2018 AHRQ MEPS DATA USERS WORKSHOP")
    print("EXERCISE 3a: USE AND EXPENDITURES FOR PERSONS WITH DIABETES, 2015")
    print("=" * 70)

    # Load data files
    cond_path = "C:/MEPS/h180.sas7bdat"
    fyc_path = "C:/MEPS/h181.sas7bdat"

    print(f"\nLoading Conditions data from: {cond_path}")
    print(f"Loading FYC data from: {fyc_path}")

    try:
        cond = load_meps_data(cond_path)
        fyc = load_meps_data(fyc_path)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please download the required files from the MEPS website:")
        print("https://meps.ahrq.gov/mepsweb/data_stats/download_data_files.jsp")
        return

    # Step 1: Pull out conditions with diabetes (CCS CODE='049', '050')
    print("\n" + "-" * 70)
    print("Step 1: Identify diabetes conditions (CCS CODE = 049 or 050)")
    print("-" * 70)

    # Check for CCCODEX column (CCS code)
    if 'CCCODEX' in cond.columns:
        diab = cond[cond['CCCODEX'].isin(['049', '050'])].copy()
    elif 'CCS' in cond.columns:
        diab = cond[cond['CCS'].isin(['049', '050'])].copy()
    else:
        print("Error: Cannot find CCS code column in conditions file")
        print(f"Available columns: {list(cond.columns)[:20]}")
        return

    print(f"Number of diabetes condition records: {len(diab):,}")

    # Check CCS codes
    ccs_col = 'CCCODEX' if 'CCCODEX' in diab.columns else 'CCS'
    print(f"\nCCS codes for diabetic conditions:")
    print(diab[ccs_col].value_counts())

    # Step 2: Identify persons who reported diabetes
    print("\n" + "-" * 70)
    print("Step 2: Identify unique persons with diabetes")
    print("-" * 70)

    diabpers = diab[['DUPERSID']].drop_duplicates()
    print(f"Number of unique persons with diabetes: {len(diabpers):,}")

    # Step 3: Create a flag for persons with diabetes in the FYC data
    print("\n" + "-" * 70)
    print("Step 3: Merge with Full-Year file and create diabetes flag")
    print("-" * 70)

    # Add flag to diabpers
    diabpers['DIABPERS'] = 1

    # Merge with FYC
    fy = fyc.merge(diabpers, on='DUPERSID', how='left')
    fy['DIABPERS'] = fy['DIABPERS'].fillna(2).astype(int)

    # Labels
    diabpers_labels = {1: 'Yes', 2: 'No'}
    sex_labels = {1: 'Male', 2: 'Female'}

    print(f"\nUnweighted # of persons who reported diabetes, 2015:")
    print(fy['DIABPERS'].map(diabpers_labels).value_counts())

    print(f"\nUnweighted # by sex:")
    print(pd.crosstab(fy['DIABPERS'].map(diabpers_labels),
                      fy['SEX'].map(sex_labels), margins=True))

    # Weighted counts
    print(f"\nWeighted # of persons who reported diabetes, 2015:")
    for val, label in diabpers_labels.items():
        subset = fy[fy['DIABPERS'] == val]
        weighted_count = subset['PERWT15F'].sum()
        print(f"  {label}: {weighted_count:,.0f}")

    # Step 4: Calculate estimates on use and expenditures for persons with diabetes
    print("\n" + "=" * 70)
    print("ESTIMATES ON USE AND EXPENDITURES FOR PERSONS WITH DIABETES, 2015")
    print("=" * 70)

    # Filter to persons with diabetes
    fy_diab = fy[fy['DIABPERS'] == 1].copy()

    # Create survey design
    design = MEPSSurveyDesign(fy_diab, year=2015)

    # Variables to analyze
    analysis_vars = ['TOTEXP15', 'TOTSLF15', 'OBTOTV15']
    var_labels = {
        'TOTEXP15': 'Total Expenditures',
        'TOTSLF15': 'Total Self/Family Payments',
        'OBTOTV15': 'Office-Based Visits'
    }

    # Overall estimates for persons with diabetes
    print("\n" + "-" * 70)
    print("Overall Estimates (Persons with Diabetes)")
    print("-" * 70)

    print(f"\n{'Variable':25s} {'N':>10s} {'Population':>15s} {'Sum':>18s} {'Mean':>12s} {'SE Mean':>10s}")
    print("-" * 95)

    for var in analysis_vars:
        if var not in fy_diab.columns:
            print(f"  {var}: Variable not found")
            continue

        total_results = design.total(var)
        mean_results = design.mean(var)

        label = var_labels.get(var, var)
        print(f"  {label:23s} {total_results['N'].values[0]:>10,.0f} "
              f"{total_results['SumWgt'].values[0]:>15,.0f} "
              f"{total_results['Total'].values[0]:>18,.0f} "
              f"{mean_results['Mean'].values[0]:>12,.2f} "
              f"{mean_results['SE'].values[0]:>10.2f}")

    # Estimates by sex for persons with diabetes
    print("\n" + "-" * 70)
    print("Estimates by Sex (Persons with Diabetes)")
    print("-" * 70)

    for sex_val, sex_label in sex_labels.items():
        print(f"\n{sex_label}:")
        subset = fy_diab[fy_diab['SEX'] == sex_val].copy()

        if len(subset) == 0:
            print("  No data")
            continue

        design_sex = MEPSSurveyDesign(subset, year=2015)

        print(f"  {'Variable':23s} {'N':>10s} {'Population':>15s} {'Sum':>18s} {'Mean':>12s} {'SE Mean':>10s}")
        print("  " + "-" * 90)

        for var in analysis_vars:
            if var not in subset.columns:
                continue

            total_results = design_sex.total(var)
            mean_results = design_sex.mean(var)

            label = var_labels.get(var, var)
            print(f"  {label:23s} {total_results['N'].values[0]:>10,.0f} "
                  f"{total_results['SumWgt'].values[0]:>15,.0f} "
                  f"{total_results['Total'].values[0]:>18,.0f} "
                  f"{mean_results['Mean'].values[0]:>12,.2f} "
                  f"{mean_results['SE'].values[0]:>10.2f}")

    print("\n" + "=" * 70)
    print("Analysis complete.")
    print("=" * 70)


if __name__ == "__main__":
    main()
