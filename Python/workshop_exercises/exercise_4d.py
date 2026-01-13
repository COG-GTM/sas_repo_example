"""
MEPS Workshop Exercise 4d: Pooling MEPS FYC Files, 2017-2019

This program demonstrates pooling 2017-2019 data using the Pooled Linkage
Variance file for correct standard error calculation. This is required
when pooling data before and after 2019 due to the CAPI redesign.

Input files:
  - C:/MEPS/h201.dta (2017 Full-Year Consolidated file)
  - C:/MEPS/h209.dta (2018 Full-Year Consolidated file)
  - C:/MEPS/h216.dta (2019 Full-Year Consolidated file)
  - C:/MEPS/h36u19.dta (Pooled Linkage Variance file)

This is the Python equivalent of the SAS program Exercise4d.sas
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.meps_loader import load_meps_data
from utils.survey_design import MEPSSurveyDesign, create_pooled_design


def main():
    # Read in data files
    fyc17 = load_meps_data("C:/MEPS/h201.dta")  # 2017 FYC
    fyc18 = load_meps_data("C:/MEPS/h209.dta")  # 2018 FYC
    fyc19 = load_meps_data("C:/MEPS/h216.dta")  # 2019 FYC
    linkage = load_meps_data("C:/MEPS/h36u19.dta")  # Pooled Variance Linkage
    
    print("=" * 70)
    print("POOLING MEPS FYC FILES, 2017-2019")
    print("Using Pooled Linkage Variance file for correct standard errors")
    print("=" * 70)
    
    # Select and standardize variables for each year
    # 2017
    fyc17_sub = fyc17[['DUPERSID', 'PANEL', 'VARSTR', 'VARPSU', 'PERWT17F',
                       'AGELAST', 'TOTEXP17', 'JTPAIN31']].copy()
    fyc17_sub = fyc17_sub.rename(columns={
        'PERWT17F': 'PERWT',
        'TOTEXP17': 'TOTEXP',
        'JTPAIN31': 'JTPAIN'
    })
    fyc17_sub['YEAR'] = 2017
    
    # 2018
    fyc18_sub = fyc18[['DUPERSID', 'PANEL', 'VARSTR', 'VARPSU', 'PERWT18F',
                       'AGELAST', 'TOTEXP18', 'JTPAIN31_M18']].copy()
    fyc18_sub = fyc18_sub.rename(columns={
        'PERWT18F': 'PERWT',
        'TOTEXP18': 'TOTEXP',
        'JTPAIN31_M18': 'JTPAIN'
    })
    fyc18_sub['YEAR'] = 2018
    
    # 2019
    fyc19_sub = fyc19[['DUPERSID', 'PANEL', 'VARSTR', 'VARPSU', 'PERWT19F',
                       'AGELAST', 'TOTEXP19', 'JTPAIN31_M18']].copy()
    fyc19_sub = fyc19_sub.rename(columns={
        'PERWT19F': 'PERWT',
        'TOTEXP19': 'TOTEXP',
        'JTPAIN31_M18': 'JTPAIN'
    })
    fyc19_sub['YEAR'] = 2019
    
    # Stack the three years
    pooled = pd.concat([fyc17_sub, fyc18_sub, fyc19_sub], ignore_index=True)
    
    # Create pooled weight (divide by number of years)
    pooled['POOLWT'] = pooled['PERWT'] / 3
    
    # Merge with Pooled Variance Linkage file
    # This file contains PSU9619 and STRA9619 for correct variance estimation
    pooled = pooled.merge(
        linkage[['DUPERSID', 'PANEL', 'PSU9619', 'STRA9619']],
        on=['DUPERSID', 'PANEL'],
        how='left'
    )
    
    # Create joint pain indicator
    pooled['has_joint_pain'] = (pooled['JTPAIN'] == 1).astype(int)
    pooled['valid_response'] = (pooled['JTPAIN'].isin([1, 2])).astype(int)
    
    print(f"\nTotal pooled observations: {len(pooled):,}")
    print(f"Observations with linkage info: {pooled['PSU9619'].notna().sum():,}")
    print(f"Valid joint pain responses: {pooled['valid_response'].sum():,}")
    
    # Define survey design using pooled variance variables
    # IMPORTANT: Use PSU9619 and STRA9619 from linkage file, not VARPSU/VARSTR
    design = MEPSSurveyDesign(
        data=pooled,
        strata='STRA9619',
        cluster='PSU9619',
        weights='POOLWT'
    )
    
    # Subset to valid responses
    design_valid = design.subset(pooled['valid_response'] == 1)
    
    # Proportion with joint pain
    print("\n" + "-" * 70)
    print("PROPORTION WITH JOINT PAIN (2017-2019 pooled)")
    print("-" * 70)
    
    pct_pain = design_valid.mean('has_joint_pain')
    print(f"Proportion: {pct_pain['mean'].values[0]:.4f}")
    print(f"SE: {pct_pain['se'].values[0]:.5f}")
    
    # Number of persons with joint pain
    print("\n" + "-" * 70)
    print("NUMBER OF PERSONS WITH JOINT PAIN")
    print("-" * 70)
    
    n_pain = design_valid.total('has_joint_pain')
    print(f"Total: {n_pain['total'].values[0]:,.0f}")
    print(f"SE: {n_pain['se'].values[0]:,.0f}")
    
    # Mean expenditures for persons with joint pain
    print("\n" + "-" * 70)
    print("MEAN EXPENDITURES FOR PERSONS WITH JOINT PAIN")
    print("-" * 70)
    
    design_pain = design.subset(pooled['has_joint_pain'] == 1)
    mean_exp = design_pain.mean('TOTEXP')
    print(f"Mean: ${mean_exp['mean'].values[0]:,.2f}")
    print(f"SE: ${mean_exp['se'].values[0]:,.2f}")
    
    # Compare by year
    print("\n" + "-" * 70)
    print("PROPORTION WITH JOINT PAIN BY YEAR")
    print("-" * 70)
    
    for year in [2017, 2018, 2019]:
        year_mask = (pooled['YEAR'] == year) & (pooled['valid_response'] == 1)
        design_year = design.subset(year_mask)
        result = design_year.mean('has_joint_pain')
        print(f"{year}: {result['mean'].values[0]:.4f} (SE: {result['se'].values[0]:.5f})")


if __name__ == "__main__":
    main()
