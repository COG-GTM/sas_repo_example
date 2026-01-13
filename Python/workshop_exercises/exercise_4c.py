"""
MEPS Workshop Exercise 4c: Pooling MEPS FYC Files, 2017 and 2018

This program demonstrates pooling 2017 and 2018 data to analyze
people with joint pain, handling variable name changes between years.

Note: JTPAIN31 variable name changed to JTPAIN31_M18 in 2018

Input files:
  - C:/MEPS/h201.dta (2017 Full-Year Consolidated file)
  - C:/MEPS/h209.dta (2018 Full-Year Consolidated file)

This is the Python equivalent of the SAS program Exercise4c.sas
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.meps_loader import load_meps_data
from utils.survey_design import MEPSSurveyDesign


def main():
    # Read in data files (using .dta format for 2017+)
    fyc17 = load_meps_data("C:/MEPS/h201.dta")  # 2017 FYC
    fyc18 = load_meps_data("C:/MEPS/h209.dta")  # 2018 FYC
    
    print("=" * 60)
    print("POOLING MEPS FYC FILES, 2017 AND 2018")
    print("People with joint pain")
    print("=" * 60)
    
    # Select and standardize variables for 2017
    # Note: Joint pain variable is JTPAIN31 in 2017
    fyc17_sub = fyc17[['DUPERSID', 'VARSTR', 'VARPSU', 'PERWT17F',
                       'AGELAST', 'TOTEXP17', 'JTPAIN31']].copy()
    fyc17_sub = fyc17_sub.rename(columns={
        'PERWT17F': 'PERWT',
        'TOTEXP17': 'TOTEXP',
        'JTPAIN31': 'JTPAIN'
    })
    fyc17_sub['YEAR'] = 2017
    
    # Select and standardize variables for 2018
    # Note: Joint pain variable is JTPAIN31_M18 in 2018
    fyc18_sub = fyc18[['DUPERSID', 'VARSTR', 'VARPSU', 'PERWT18F',
                       'AGELAST', 'TOTEXP18', 'JTPAIN31_M18']].copy()
    fyc18_sub = fyc18_sub.rename(columns={
        'PERWT18F': 'PERWT',
        'TOTEXP18': 'TOTEXP',
        'JTPAIN31_M18': 'JTPAIN'
    })
    fyc18_sub['YEAR'] = 2018
    
    # Stack the two years
    pooled = pd.concat([fyc17_sub, fyc18_sub], ignore_index=True)
    
    # Create pooled weight
    pooled['POOLWT'] = pooled['PERWT'] / 2
    
    # Create joint pain indicator
    # JTPAIN: 1 = Yes, 2 = No, -1 = Inapplicable, -7/-8/-9 = Missing
    pooled['has_joint_pain'] = (pooled['JTPAIN'] == 1).astype(int)
    
    # Create subpopulation for valid responses (adults with valid JTPAIN response)
    pooled['valid_response'] = (pooled['JTPAIN'].isin([1, 2])).astype(int)
    
    print(f"\nTotal pooled observations: {len(pooled):,}")
    print(f"Valid joint pain responses: {pooled['valid_response'].sum():,}")
    print(f"Persons with joint pain: {pooled['has_joint_pain'].sum():,}")
    
    # Define survey design
    design = MEPSSurveyDesign(
        data=pooled,
        strata='VARSTR',
        cluster='VARPSU',
        weights='POOLWT'
    )
    
    # Subset to valid responses
    design_valid = design.subset(pooled['valid_response'] == 1)
    
    # Proportion with joint pain
    print("\n" + "-" * 60)
    print("PROPORTION WITH JOINT PAIN")
    print("-" * 60)
    
    pct_pain = design_valid.mean('has_joint_pain')
    print(f"Proportion: {pct_pain['mean'].values[0]:.4f}")
    print(f"SE: {pct_pain['se'].values[0]:.5f}")
    
    # Number of persons with joint pain
    print("\n" + "-" * 60)
    print("NUMBER OF PERSONS WITH JOINT PAIN")
    print("-" * 60)
    
    n_pain = design_valid.total('has_joint_pain')
    print(f"Total: {n_pain['total'].values[0]:,.0f}")
    print(f"SE: {n_pain['se'].values[0]:,.0f}")
    
    # Mean expenditures for persons with joint pain
    print("\n" + "-" * 60)
    print("MEAN EXPENDITURES FOR PERSONS WITH JOINT PAIN")
    print("-" * 60)
    
    design_pain = design.subset(pooled['has_joint_pain'] == 1)
    mean_exp = design_pain.mean('TOTEXP')
    print(f"Mean: ${mean_exp['mean'].values[0]:,.2f}")
    print(f"SE: ${mean_exp['se'].values[0]:,.2f}")


if __name__ == "__main__":
    main()
