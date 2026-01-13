"""
MEPS Summary Tables Example: Health Insurance Coverage, 2016

This code replicates estimates from the MEPS-HC Data Tools summary tables for
Health Insurance: Insurance Coverage, 2016

Estimates:
  - Number and percent of people by insurance coverage
  - By age group

Input file: C:/MEPS/h192.ssp (2016 Full-Year Consolidated file)

This is the Python equivalent of ins_coverage_2016.sas
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.meps_loader import load_meps_data
from utils.survey_design import MEPSSurveyDesign


def main():
    # Load FYC file
    fyc = load_meps_data("C:/MEPS/h192.ssp")
    
    print("=" * 70)
    print("HEALTH INSURANCE COVERAGE, 2016")
    print("=" * 70)
    
    # Insurance coverage variable: INSCOV16
    # 1 = Any private, 2 = Public only, 3 = Uninsured
    
    # Create indicator variables
    fyc['private'] = (fyc['INSCOV16'] == 1).astype(int)
    fyc['public'] = (fyc['INSCOV16'] == 2).astype(int)
    fyc['uninsured'] = (fyc['INSCOV16'] == 3).astype(int)
    
    # Create age categories
    fyc['AGE_CAT'] = pd.cut(
        fyc['AGELAST'],
        bins=[-1, 17, 44, 64, 200],
        labels=['Under 18', '18-44', '45-64', '65+']
    )
    
    print(f"\nTotal observations: {len(fyc):,}")
    
    # Define survey design
    design = MEPSSurveyDesign(
        data=fyc,
        strata='VARSTR',
        cluster='VARPSU',
        weights='PERWT16F'
    )
    
    # Overall coverage
    print("\n" + "-" * 70)
    print("INSURANCE COVERAGE (Overall)")
    print("-" * 70)
    
    coverage_vars = {
        'private': 'Any private',
        'public': 'Public only',
        'uninsured': 'Uninsured'
    }
    
    for var, label in coverage_vars.items():
        result_mean = design.mean(var)
        result_total = design.total(var)
        print(f"\n{label}:")
        print(f"  Number: {result_total['total'].values[0]:,.0f} (SE: {result_total['se'].values[0]:,.0f})")
        print(f"  Percent: {result_mean['mean'].values[0]:.4f} (SE: {result_mean['se'].values[0]:.5f})")
    
    # By age group
    print("\n" + "-" * 70)
    print("UNINSURED BY AGE GROUP")
    print("-" * 70)
    
    by_age_mean = design.mean('uninsured', domain='AGE_CAT')
    by_age_total = design.total('uninsured', domain='AGE_CAT')
    
    for _, row in by_age_mean.iterrows():
        total_row = by_age_total[by_age_total['domain_value'] == row['domain_value']].iloc[0]
        print(f"\n{row['domain_value']}:")
        print(f"  Number: {total_row['total']:,.0f} (SE: {total_row['se']:,.0f})")
        print(f"  Percent: {row['mean']:.4f} (SE: {row['se']:.5f})")


if __name__ == "__main__":
    main()
