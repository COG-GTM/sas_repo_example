"""
MEPS Workshop Exercise 1b: National Health Care Expenses by Age Group and Type of Service, 2015

This program generates estimates on national health care expenses by age group
and type of service (office-based, outpatient, inpatient, etc.)

Input file: C:/MEPS/h181.ssp (2015 Full-Year Consolidated file)

This is the Python equivalent of the SAS program Exercise1b.sas
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.meps_loader import load_meps_data
from utils.survey_design import MEPSSurveyDesign


def main():
    # Read in data from 2015 consolidated data file (HC-181)
    h181 = load_meps_data("C:/MEPS/h181.ssp")
    
    # Define expenditure variables by type of service
    exp_vars = {
        'TOTEXP15': 'Total',
        'OBVEXP15': 'Office-Based',
        'OPTEXP15': 'Outpatient',
        'ERTEXP15': 'Emergency Room',
        'IPTEXP15': 'Inpatient',
        'RXEXP15': 'Prescription Medicines',
        'DVTEXP15': 'Dental',
        'HHAEXP15': 'Home Health',
        'OTHEXP15': 'Other'
    }
    
    # Create age category variable
    h181['AGECAT'] = pd.cut(
        h181['AGELAST'],
        bins=[-1, 17, 44, 64, 200],
        labels=['0-17', '18-44', '45-64', '65+']
    )
    
    # Create flag for persons with any expense
    h181['has_exp'] = (h181['TOTEXP15'] > 0).astype(int)
    
    # Define the survey design
    design = MEPSSurveyDesign(
        data=h181,
        strata='VARSTR',
        cluster='VARPSU',
        weights='PERWT15F'
    )
    
    print("=" * 70)
    print("NATIONAL HEALTH CARE EXPENSES BY TYPE OF SERVICE, 2015")
    print("=" * 70)
    
    # Total expenses by type of service
    print("\nTOTAL EXPENSES BY TYPE OF SERVICE (in millions)")
    print("-" * 70)
    
    for var, label in exp_vars.items():
        result = design.total(var)
        total_millions = result['total'].values[0] / 1e6
        se_millions = result['se'].values[0] / 1e6
        print(f"{label:25s}: ${total_millions:>12,.0f}M  (SE: ${se_millions:>10,.0f}M)")
    
    # Mean expenses by age group
    print("\n" + "=" * 70)
    print("MEAN TOTAL EXPENSES BY AGE GROUP")
    print("-" * 70)
    
    mean_by_age = design.mean('TOTEXP15', domain='AGECAT')
    for _, row in mean_by_age.iterrows():
        print(f"Age {row['domain_value']:>6s}: ${row['mean']:>10,.2f}  (SE: ${row['se']:>8,.2f})")
    
    # Mean expenses by type of service for persons with any expense
    print("\n" + "=" * 70)
    print("MEAN EXPENSES BY TYPE OF SERVICE (Persons with any expense)")
    print("-" * 70)
    
    design_with_exp = design.subset(h181['has_exp'] == 1)
    
    for var, label in exp_vars.items():
        result = design_with_exp.mean(var)
        print(f"{label:25s}: ${result['mean'].values[0]:>10,.2f}  (SE: ${result['se'].values[0]:>8,.2f})")


if __name__ == "__main__":
    main()
