"""
MEPS Workshop Exercise 1a: National Health Care Expenses, 2016

This program generates the following estimates on national health care expenses:
    (1) Overall expenses
    (2) Percentage of persons with an expense
    (3) Mean expense per person with an expense

Input file: C:/MEPS/h192.ssp (2016 Full-Year Consolidated file)

This is the Python equivalent of the SAS program Exercise1a.sas
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.meps_loader import load_meps_data
from utils.survey_design import MEPSSurveyDesign


def main():
    # Read in data from 2016 consolidated data file (HC-192)
    # Update the path to match your local MEPS data directory
    h192 = load_meps_data("C:/MEPS/h192.ssp")
    
    # Create variables for analysis
    h192['TOTAL'] = h192['TOTEXP16']
    
    # Create flag (1/0) variable for persons with an expense
    h192['has_exp'] = (h192['TOTAL'] > 0).astype(int)
    
    # Create age variable from end of year, round 42, and round 31 variables
    h192['AGE'] = np.where(
        h192['AGE16X'] >= 0, h192['AGE16X'],
        np.where(h192['AGE42X'] >= 0, h192['AGE42X'], h192['AGE31X'])
    )
    
    # Create age category: 1 = 0-64, 2 = 65+
    h192['AGECAT'] = np.where(h192['AGE'] <= 64, '0-64', '65+')
    
    # QC check on new variables
    print("=" * 60)
    print("QC: Expense flag by total expenses")
    print(h192.groupby('has_exp')['TOTAL'].agg(['min', 'max', 'count']))
    
    print("\nQC: Age category by age")
    print(h192.groupby('AGECAT')['AGE'].agg(['min', 'max', 'count']))
    
    # Define the survey design
    design = MEPSSurveyDesign(
        data=h192,
        strata='VARSTR',
        cluster='VARPSU',
        weights='PERWT16F'
    )
    
    print("\n" + "=" * 60)
    print("Survey Design Summary:")
    print(design.describe())
    
    # Overall expenses
    print("\n" + "=" * 60)
    print("OVERALL EXPENSES")
    print("-" * 60)
    
    total_results = design.total('TOTAL')
    print(f"Total Expense: ${total_results['total'].values[0]:,.0f}")
    print(f"SE of Total: ${total_results['se'].values[0]:,.0f}")
    
    mean_results = design.mean('TOTAL')
    print(f"\nMean Expense: ${mean_results['mean'].values[0]:,.2f}")
    print(f"SE of Mean: ${mean_results['se'].values[0]:,.2f}")
    
    # Percentage of persons with an expense
    print("\n" + "=" * 60)
    print("PERCENTAGE OF PERSONS WITH AN EXPENSE")
    print("-" * 60)
    
    pct_results = design.mean('has_exp')
    print(f"Proportion with expense: {pct_results['mean'].values[0]:.4f}")
    print(f"SE of Proportion: {pct_results['se'].values[0]:.5f}")
    
    # Number of persons with an expense
    total_with_exp = design.total('has_exp')
    print(f"\nNumber with expense: {total_with_exp['total'].values[0]:,.0f}")
    print(f"SE: {total_with_exp['se'].values[0]:,.0f}")
    
    # Mean expense per person with an expense
    print("\n" + "=" * 60)
    print("MEAN EXPENSE PER PERSON WITH AN EXPENSE")
    print("-" * 60)
    
    # Create subset design for persons with expenses
    design_with_exp = design.subset(h192['has_exp'] == 1)
    
    # Overall mean for persons with expense
    mean_with_exp = design_with_exp.mean('TOTAL')
    print(f"Overall Mean (persons with expense): ${mean_with_exp['mean'].values[0]:,.2f}")
    print(f"SE: ${mean_with_exp['se'].values[0]:,.2f}")
    
    # Mean by age category for persons with expense
    print("\nMean by Age Category (persons with expense):")
    mean_by_age = design_with_exp.mean('TOTAL', domain='AGECAT')
    for _, row in mean_by_age.iterrows():
        print(f"  {row['domain_value']}: ${row['mean']:,.2f} (SE: ${row['se']:,.2f})")


if __name__ == "__main__":
    main()
