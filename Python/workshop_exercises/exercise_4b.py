"""
MEPS Workshop Exercise 4b: Pooling Longitudinal Files, Panels 17-19

This program demonstrates pooling longitudinal files across multiple panels
to analyze changes in health insurance coverage.

Input files:
  - C:/MEPS/h172.ssp (Panel 17 Longitudinal file)
  - C:/MEPS/h183.ssp (Panel 18 Longitudinal file)
  - C:/MEPS/h193.ssp (Panel 19 Longitudinal file)

This is the Python equivalent of the SAS program Exercise4b.sas
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.meps_loader import load_meps_data
from utils.survey_design import MEPSSurveyDesign


def main():
    # Read in longitudinal files
    panel17 = load_meps_data("C:/MEPS/h172.ssp")  # Panel 17
    panel18 = load_meps_data("C:/MEPS/h183.ssp")  # Panel 18
    panel19 = load_meps_data("C:/MEPS/h193.ssp")  # Panel 19
    
    print("=" * 60)
    print("POOLING LONGITUDINAL FILES, PANELS 17-19")
    print("=" * 60)
    
    # Select and standardize variables for each panel
    # Panel 17 (2012-2013)
    p17_sub = panel17[['DUPERSID', 'VARSTR', 'VARPSU', 'LONGWT', 
                       'INSCOVY1', 'INSCOVY2']].copy()
    p17_sub['PANEL'] = 17
    
    # Panel 18 (2013-2014)
    p18_sub = panel18[['DUPERSID', 'VARSTR', 'VARPSU', 'LONGWT',
                       'INSCOVY1', 'INSCOVY2']].copy()
    p18_sub['PANEL'] = 18
    
    # Panel 19 (2014-2015)
    p19_sub = panel19[['DUPERSID', 'VARSTR', 'VARPSU', 'LONGWT',
                       'INSCOVY1', 'INSCOVY2']].copy()
    p19_sub['PANEL'] = 19
    
    # Stack panels
    pooled = pd.concat([p17_sub, p18_sub, p19_sub], ignore_index=True)
    
    # Create pooled weight
    pooled['POOLWT'] = pooled['LONGWT'] / 3
    
    # Create insurance change variable
    # INSCOV: 1 = Any private, 2 = Public only, 3 = Uninsured
    pooled['ins_change'] = np.where(
        pooled['INSCOVY1'] != pooled['INSCOVY2'],
        'Changed',
        'No Change'
    )
    
    # Create specific change categories
    pooled['change_type'] = 'No Change'
    pooled.loc[(pooled['INSCOVY1'] == 3) & (pooled['INSCOVY2'] != 3), 'change_type'] = 'Gained Coverage'
    pooled.loc[(pooled['INSCOVY1'] != 3) & (pooled['INSCOVY2'] == 3), 'change_type'] = 'Lost Coverage'
    pooled.loc[(pooled['INSCOVY1'] == 1) & (pooled['INSCOVY2'] == 2), 'change_type'] = 'Private to Public'
    pooled.loc[(pooled['INSCOVY1'] == 2) & (pooled['INSCOVY2'] == 1), 'change_type'] = 'Public to Private'
    
    print(f"\nTotal pooled observations: {len(pooled):,}")
    print("\nInsurance change distribution:")
    print(pooled['change_type'].value_counts())
    
    # Define survey design
    design = MEPSSurveyDesign(
        data=pooled,
        strata='VARSTR',
        cluster='VARPSU',
        weights='POOLWT'
    )
    
    # Proportion with insurance change
    print("\n" + "-" * 60)
    print("PROPORTION WITH INSURANCE CHANGE")
    print("-" * 60)
    
    pooled['changed'] = (pooled['ins_change'] == 'Changed').astype(int)
    design = MEPSSurveyDesign(
        data=pooled,
        strata='VARSTR',
        cluster='VARPSU',
        weights='POOLWT'
    )
    
    pct_change = design.mean('changed')
    print(f"Proportion: {pct_change['mean'].values[0]:.4f}")
    print(f"SE: {pct_change['se'].values[0]:.5f}")
    
    # Proportions by change type
    print("\n" + "-" * 60)
    print("PROPORTIONS BY CHANGE TYPE")
    print("-" * 60)
    
    for change_type in ['No Change', 'Gained Coverage', 'Lost Coverage', 
                        'Private to Public', 'Public to Private']:
        pooled[f'is_{change_type}'] = (pooled['change_type'] == change_type).astype(int)
        result = design.mean(f'is_{change_type}')
        print(f"{change_type:20s}: {result['mean'].values[0]:.4f} (SE: {result['se'].values[0]:.5f})")


if __name__ == "__main__":
    main()
