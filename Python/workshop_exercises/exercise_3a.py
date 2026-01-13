"""
MEPS Workshop Exercise 3a: Use and Expenditures for Persons with Diabetes, 2015

This program analyzes healthcare use and expenditures for persons with diabetes,
including total expenditures, office-based visits, and prescription medicines.

Input files:
  - C:/MEPS/h181.ssp (2015 Full-Year Consolidated file)
  - C:/MEPS/h180.ssp (2015 Medical Conditions file)

This is the Python equivalent of the SAS program Exercise3a.sas
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.meps_loader import load_meps_data
from utils.survey_design import MEPSSurveyDesign


def main():
    # Read in data files
    fyc = load_meps_data("C:/MEPS/h181.ssp")  # 2015 FYC
    cond = load_meps_data("C:/MEPS/h180.ssp")  # 2015 Conditions
    
    # CCS code for diabetes (049, 050)
    # 049 = Diabetes mellitus without complication
    # 050 = Diabetes mellitus with complication
    DIABETES_CCS = ['049', '050']
    
    # Identify persons with diabetes from conditions file
    diabetes_cond = cond[cond['CCCODEX'].isin(DIABETES_CCS)].copy()
    
    # Get unique person IDs with diabetes
    diabetes_persons = diabetes_cond['DUPERSID'].unique()
    
    print("=" * 60)
    print("USE AND EXPENDITURES FOR PERSONS WITH DIABETES, 2015")
    print("=" * 60)
    
    print(f"\nNumber of diabetes condition records: {len(diabetes_cond):,}")
    print(f"Number of unique persons with diabetes: {len(diabetes_persons):,}")
    
    # Create diabetes flag in FYC file
    fyc['has_diabetes'] = fyc['DUPERSID'].isin(diabetes_persons).astype(int)
    
    # Define survey design
    design = MEPSSurveyDesign(
        data=fyc,
        strata='VARSTR',
        cluster='VARPSU',
        weights='PERWT15F'
    )
    
    # Number of persons with diabetes
    print("\n" + "-" * 60)
    print("NUMBER OF PERSONS WITH DIABETES")
    n_diabetes = design.total('has_diabetes')
    print(f"Total: {n_diabetes['total'].values[0]:,.0f}")
    print(f"SE: {n_diabetes['se'].values[0]:,.0f}")
    
    # Create subset design for persons with diabetes
    design_diabetes = design.subset(fyc['has_diabetes'] == 1)
    
    # Total expenditures for persons with diabetes
    print("\n" + "-" * 60)
    print("TOTAL EXPENDITURES FOR PERSONS WITH DIABETES")
    total_exp = design_diabetes.total('TOTEXP15')
    print(f"Total: ${total_exp['total'].values[0]:,.0f}")
    print(f"SE: ${total_exp['se'].values[0]:,.0f}")
    
    # Mean expenditure per person with diabetes
    print("\n" + "-" * 60)
    print("MEAN EXPENDITURE PER PERSON WITH DIABETES")
    mean_exp = design_diabetes.mean('TOTEXP15')
    print(f"Mean: ${mean_exp['mean'].values[0]:,.2f}")
    print(f"SE: ${mean_exp['se'].values[0]:,.2f}")
    
    # Expenditures by type of service
    print("\n" + "-" * 60)
    print("MEAN EXPENDITURE BY TYPE OF SERVICE (Persons with diabetes)")
    
    exp_vars = {
        'TOTEXP15': 'Total',
        'OBVEXP15': 'Office-Based',
        'OPTEXP15': 'Outpatient',
        'ERTEXP15': 'Emergency Room',
        'IPTEXP15': 'Inpatient',
        'RXEXP15': 'Prescription Medicines'
    }
    
    for var, label in exp_vars.items():
        result = design_diabetes.mean(var)
        print(f"{label:25s}: ${result['mean'].values[0]:>10,.2f}  (SE: ${result['se'].values[0]:>8,.2f})")


if __name__ == "__main__":
    main()
