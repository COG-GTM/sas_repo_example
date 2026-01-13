"""
MEPS Workshop Exercise 4a: Pooling MEPS FYC Files, 2015 and 2016

This program demonstrates pooling multiple years of MEPS data to analyze
out-of-pocket expenditures for uninsured persons ages 26-30 with high income.

Input files:
  - C:/MEPS/h181.ssp (2015 Full-Year Consolidated file)
  - C:/MEPS/h192.ssp (2016 Full-Year Consolidated file)

This is the Python equivalent of the SAS program Exercise4a.sas
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
    fyc15 = load_meps_data("C:/MEPS/h181.ssp")  # 2015 FYC
    fyc16 = load_meps_data("C:/MEPS/h192.ssp")  # 2016 FYC
    
    print("=" * 60)
    print("POOLING MEPS FYC FILES, 2015 AND 2016")
    print("Out-of-pocket expenditures for uninsured persons ages 26-30")
    print("with high income")
    print("=" * 60)
    
    # Select and rename variables for 2015
    fyc15_sub = fyc15[['DUPERSID', 'VARSTR', 'VARPSU', 'PERWT15F', 
                       'AGELAST', 'INSCOV15', 'POVCAT15', 'TOTSLF15']].copy()
    fyc15_sub = fyc15_sub.rename(columns={
        'PERWT15F': 'PERWT',
        'INSCOV15': 'INSCOV',
        'POVCAT15': 'POVCAT',
        'TOTSLF15': 'TOTSLF'
    })
    fyc15_sub['YEAR'] = 2015
    
    # Select and rename variables for 2016
    fyc16_sub = fyc16[['DUPERSID', 'VARSTR', 'VARPSU', 'PERWT16F',
                       'AGELAST', 'INSCOV16', 'POVCAT16', 'TOTSLF16']].copy()
    fyc16_sub = fyc16_sub.rename(columns={
        'PERWT16F': 'PERWT',
        'INSCOV16': 'INSCOV',
        'POVCAT16': 'POVCAT',
        'TOTSLF16': 'TOTSLF'
    })
    fyc16_sub['YEAR'] = 2016
    
    # Stack the two years
    pooled = pd.concat([fyc15_sub, fyc16_sub], ignore_index=True)
    
    # Create pooled weight (divide by number of years)
    pooled['POOLWT'] = pooled['PERWT'] / 2
    
    # Define subpopulation:
    # - Ages 26-30
    # - Uninsured all year (INSCOV = 3)
    # - High income (POVCAT = 5)
    pooled['subpop'] = (
        (pooled['AGELAST'] >= 26) & 
        (pooled['AGELAST'] <= 30) &
        (pooled['INSCOV'] == 3) &
        (pooled['POVCAT'] == 5)
    ).astype(int)
    
    print(f"\nTotal pooled observations: {len(pooled):,}")
    print(f"Subpopulation (uninsured, 26-30, high income): {pooled['subpop'].sum():,}")
    
    # Define survey design with pooled weights
    design = MEPSSurveyDesign(
        data=pooled,
        strata='VARSTR',
        cluster='VARPSU',
        weights='POOLWT'
    )
    
    # Create subset design for subpopulation
    design_subpop = design.subset(pooled['subpop'] == 1)
    
    # Mean out-of-pocket expenditures for subpopulation
    print("\n" + "-" * 60)
    print("MEAN OUT-OF-POCKET EXPENDITURES")
    print("(Uninsured persons ages 26-30 with high income)")
    print("-" * 60)
    
    mean_oop = design_subpop.mean('TOTSLF')
    print(f"Mean: ${mean_oop['mean'].values[0]:,.2f}")
    print(f"SE: ${mean_oop['se'].values[0]:,.2f}")
    
    # Total out-of-pocket expenditures
    print("\n" + "-" * 60)
    print("TOTAL OUT-OF-POCKET EXPENDITURES")
    print("-" * 60)
    
    total_oop = design_subpop.total('TOTSLF')
    print(f"Total: ${total_oop['total'].values[0]:,.0f}")
    print(f"SE: ${total_oop['se'].values[0]:,.0f}")
    
    # Number of persons in subpopulation
    pooled['person'] = 1
    design = MEPSSurveyDesign(
        data=pooled,
        strata='VARSTR',
        cluster='VARPSU',
        weights='POOLWT'
    )
    design_subpop = design.subset(pooled['subpop'] == 1)
    
    print("\n" + "-" * 60)
    print("NUMBER OF PERSONS IN SUBPOPULATION")
    print("-" * 60)
    
    n_persons = design_subpop.total('person')
    print(f"Total: {n_persons['total'].values[0]:,.0f}")
    print(f"SE: {n_persons['se'].values[0]:,.0f}")


if __name__ == "__main__":
    main()
