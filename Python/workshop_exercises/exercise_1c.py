"""
MEPS Workshop Exercise 1c: National Health Care Expenses by Age Group, 2018

This program generates the following estimates on national health care expenses:
    (1) Overall expenses
    (2) Percentage of persons with an expense
    (3) Mean expense per person with an expense

Input file: C:/MEPS/h209.sas7bdat or h209.dta (2018 Full-Year Consolidated file)

This is the Python equivalent of the SAS program Exercise1c.sas
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.meps_loader import load_meps_data
from utils.survey_design import MEPSSurveyDesign


def main():
    # Read in data from 2018 consolidated data file (HC-209)
    # For 2018+, .dta or .sas7bdat formats are recommended
    h209 = load_meps_data("C:/MEPS/h209.dta")
    
    # Create variables for analysis
    h209['TOTAL'] = h209['TOTEXP18']
    
    # Create flag (1/0) variable for persons with an expense
    h209['has_exp'] = (h209['TOTAL'] > 0).astype(int)
    
    # Create age category: 0-64, 65+
    h209['AGECAT'] = np.where(h209['AGELAST'] <= 64, '0-64', '65+')
    
    # QC check on new variables
    print("=" * 60)
    print("QC: Expense flag by total expenses")
    print(h209.groupby('has_exp')['TOTAL'].agg(['min', 'max', 'count']))
    
    print("\nQC: Age category by age")
    print(h209.groupby('AGECAT')['AGELAST'].agg(['min', 'max', 'count']))
    
    # Define the survey design
    design = MEPSSurveyDesign(
        data=h209,
        strata='VARSTR',
        cluster='VARPSU',
        weights='PERWT18F'
    )
    
    print("\n" + "=" * 60)
    print("Survey Design Summary:")
    print(design.describe())
    
    # Overall expenses
    print("\n" + "=" * 60)
    print("OVERALL EXPENSES, 2018")
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
    
    # Mean expense per person with an expense
    print("\n" + "=" * 60)
    print("MEAN EXPENSE PER PERSON WITH AN EXPENSE")
    print("-" * 60)
    
    design_with_exp = design.subset(h209['has_exp'] == 1)
    
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
