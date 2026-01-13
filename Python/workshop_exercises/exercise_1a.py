"""
Exercise 1a: National Health Care Expenses by Age Group, 2016

This program generates the following estimates on national health care expenses, 2016:
    (1) Overall expenses
    (2) Percentage of persons with an expense
    (3) Mean expense per person with an expense

Input file: C:/MEPS/h192.sas7bdat (2016 Full-Year Consolidated file)

This is a Python translation of the SAS program Exercise1a.sas
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
    print("EXERCISE 1a: NATIONAL HEALTH CARE EXPENSES, 2016")
    print("=" * 70)

    # Load 2016 Full-Year Consolidated file (HC-192)
    # Note: Update the path to match your local MEPS data directory
    filepath = "C:/MEPS/h192.sas7bdat"

    print(f"\nLoading data from: {filepath}")
    try:
        data = load_meps_data(filepath)
    except FileNotFoundError:
        print(f"Error: File not found at {filepath}")
        print("Please download the 2016 FYC file (h192) from the MEPS website:")
        print("https://meps.ahrq.gov/mepsweb/data_stats/download_data_files.jsp")
        return

    # Keep only needed variables
    keep_vars = ['TOTEXP16', 'AGE16X', 'AGE42X', 'AGE31X', 'VARSTR', 'VARPSU', 'PERWT16F']
    data = data[keep_vars].copy()

    # Create TOTAL variable
    data['TOTAL'] = data['TOTEXP16']

    # Create flag (1/0) variable for persons with an expense
    data['X_ANYSVCE'] = (data['TOTAL'] > 0).astype(int)

    # Create a summary AGE variable from end of year, 42, and 31 variables
    data['AGE'] = np.where(
        data['AGE16X'] >= 0, data['AGE16X'],
        np.where(data['AGE42X'] >= 0, data['AGE42X'],
                 np.where(data['AGE31X'] >= 0, data['AGE31X'], np.nan))
    )

    # Create age category variable
    data['AGECAT'] = np.where(
        (data['AGE'] >= 0) & (data['AGE'] <= 64), 1,
        np.where(data['AGE'] > 64, 2, np.nan)
    )

    # Define age category labels
    agecat_labels = {1: '0-64', 2: '65+'}

    # Supporting crosstabs for the flag variables
    print("\n" + "-" * 70)
    print("Supporting crosstabs for the flag variables")
    print("-" * 70)

    print("\nX_ANYSVCE by TOTAL (expense > 0):")
    print(pd.crosstab(data['X_ANYSVCE'], data['TOTAL'] > 0, margins=True))

    print("\nAGECAT by AGE range:")
    print(pd.crosstab(data['AGECAT'].map(agecat_labels),
                      pd.cut(data['AGE'], bins=[-1, 64, 200], labels=['0-64', '65+']),
                      margins=True))

    # Create survey design
    design = MEPSSurveyDesign(data, year=2016)

    # Calculate estimates
    print("\n" + "=" * 70)
    print("PERCENTAGE OF PERSONS WITH AN EXPENSE & OVERALL EXPENSES")
    print("=" * 70)

    # Percentage of persons with an expense
    print("\nPERCENTAGE OF PERSONS WITH AN EXPENSE:")
    pct_results = design.mean('X_ANYSVCE')
    print(f"  N:           {pct_results['N'].values[0]:,.0f}")
    print(f"  Population:  {pct_results['SumWgt'].values[0]:,.0f}")
    print(f"  Proportion:  {pct_results['Mean'].values[0]:.4f}")
    print(f"  SE:          {pct_results['SE'].values[0]:.5f}")

    # Total persons with any expense
    total_with_expense = design.total('X_ANYSVCE')
    print(f"  Persons with Any Expense: {total_with_expense['Total'].values[0]:,.0f}")
    print(f"  SE:                       {total_with_expense['SE'].values[0]:,.0f}")

    # Overall expenses
    print("\nOVERALL EXPENSES:")
    exp_results = design.total('TOTAL')
    print(f"  N:              {exp_results['N'].values[0]:,.0f}")
    print(f"  Population:     {exp_results['SumWgt'].values[0]:,.0f}")
    print(f"  Total Expense:  ${exp_results['Total'].values[0]:,.0f}")
    print(f"  SE:             ${exp_results['SE'].values[0]:,.0f}")

    mean_exp = design.mean('TOTAL')
    print(f"  Mean Expense:   ${mean_exp['Mean'].values[0]:,.2f}")
    print(f"  SE of Mean:     ${mean_exp['SE'].values[0]:.2f}")

    # Mean expense per person with an expense, by age group
    print("\n" + "=" * 70)
    print("MEAN EXPENSE PER PERSON WITH AN EXPENSE, BY AGE GROUP")
    print("=" * 70)

    # Filter to persons with expense
    data_with_expense = data[data['X_ANYSVCE'] == 1].copy()

    # Overall (all ages with expense)
    design_exp = MEPSSurveyDesign(data_with_expense, year=2016)
    overall_mean = design_exp.mean('TOTAL')

    print("\nAll Ages (with expense):")
    print(f"  N:           {overall_mean['N'].values[0]:,.0f}")
    print(f"  Population:  {overall_mean['SumWgt'].values[0]:,.0f}")
    print(f"  Mean($):     ${overall_mean['Mean'].values[0]:,.1f}")
    print(f"  SE:          ${overall_mean['SE'].values[0]:.4f}")

    # By age group
    for agecat_val, agecat_label in agecat_labels.items():
        subset = data_with_expense[data_with_expense['AGECAT'] == agecat_val].copy()
        if len(subset) > 0:
            design_subset = MEPSSurveyDesign(subset, year=2016)
            age_mean = design_subset.mean('TOTAL')

            print(f"\nAge {agecat_label} (with expense):")
            print(f"  N:           {age_mean['N'].values[0]:,.0f}")
            print(f"  Population:  {age_mean['SumWgt'].values[0]:,.0f}")
            print(f"  Mean($):     ${age_mean['Mean'].values[0]:,.1f}")
            print(f"  SE:          ${age_mean['SE'].values[0]:.4f}")

    print("\n" + "=" * 70)
    print("Analysis complete.")
    print("=" * 70)


if __name__ == "__main__":
    main()
