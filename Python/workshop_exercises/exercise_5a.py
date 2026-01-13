"""
MEPS Workshop Exercise 5a: Constructing Family-Level Variables, 2015

This program demonstrates how to construct family-level variables from
person-level data, including family total expenditures and family size.

Input file: C:/MEPS/h181.ssp (2015 Full-Year Consolidated file)

This is the Python equivalent of the SAS program Exercise5a.sas
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.meps_loader import load_meps_data
from utils.survey_design import MEPSSurveyDesign


def main():
    # Read in data from 2015 FYC file
    fyc = load_meps_data("C:/MEPS/h181.ssp")
    
    print("=" * 60)
    print("CONSTRUCTING FAMILY-LEVEL VARIABLES, 2015")
    print("=" * 60)
    
    # DTEFAMIDYR is the family ID variable
    # Create family-level aggregates
    family_agg = fyc.groupby('DUFAMIDYR').agg({
        'TOTEXP15': 'sum',
        'DUPERSID': 'count',
        'PERWT15F': 'first',
        'VARSTR': 'first',
        'VARPSU': 'first'
    }).reset_index()
    
    family_agg.columns = ['DUFAMIDYR', 'FAM_TOTEXP', 'FAM_SIZE', 
                          'PERWT15F', 'VARSTR', 'VARPSU']
    
    print(f"\nNumber of families: {len(family_agg):,}")
    print(f"\nFamily size distribution:")
    print(family_agg['FAM_SIZE'].value_counts().sort_index())
    
    # Merge family-level variables back to person level
    fyc_with_fam = fyc.merge(
        family_agg[['DUFAMIDYR', 'FAM_TOTEXP', 'FAM_SIZE']],
        on='DUFAMIDYR',
        how='left'
    )
    
    # Create family size categories
    fyc_with_fam['FAM_SIZE_CAT'] = pd.cut(
        fyc_with_fam['FAM_SIZE'],
        bins=[0, 1, 2, 4, 100],
        labels=['1 person', '2 persons', '3-4 persons', '5+ persons']
    )
    
    # Define survey design
    design = MEPSSurveyDesign(
        data=fyc_with_fam,
        strata='VARSTR',
        cluster='VARPSU',
        weights='PERWT15F'
    )
    
    # Mean family expenditure by family size
    print("\n" + "-" * 60)
    print("MEAN FAMILY EXPENDITURE BY FAMILY SIZE")
    print("-" * 60)
    
    mean_by_size = design.mean('FAM_TOTEXP', domain='FAM_SIZE_CAT')
    for _, row in mean_by_size.iterrows():
        print(f"{row['domain_value']:15s}: ${row['mean']:>12,.2f}  (SE: ${row['se']:>10,.2f})")
    
    # Distribution of family sizes
    print("\n" + "-" * 60)
    print("DISTRIBUTION OF FAMILY SIZES")
    print("-" * 60)
    
    for size_cat in ['1 person', '2 persons', '3-4 persons', '5+ persons']:
        fyc_with_fam[f'is_{size_cat}'] = (fyc_with_fam['FAM_SIZE_CAT'] == size_cat).astype(int)
        result = design.mean(f'is_{size_cat}')
        print(f"{size_cat:15s}: {result['mean'].values[0]:.4f} (SE: {result['se'].values[0]:.5f})")


if __name__ == "__main__":
    main()
