"""
MEPS Workshop Exercise 5b: Constructing Insurance Status from Monthly Variables, 2015

This program demonstrates how to construct insurance status variables from
monthly insurance indicators to determine coverage patterns throughout the year.

Input file: C:/MEPS/h181.ssp (2015 Full-Year Consolidated file)

This is the Python equivalent of the SAS program Exercise5b.sas
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
    print("CONSTRUCTING INSURANCE STATUS FROM MONTHLY VARIABLES, 2015")
    print("=" * 60)
    
    # Monthly insurance variables:
    # PRVEVyy = Private insurance in month yy (1=Yes, 2=No)
    # PUBEVyy = Public insurance in month yy (1=Yes, 2=No)
    
    # Create monthly coverage indicators
    months = ['JA', 'FE', 'MA', 'AP', 'MY', 'JU', 'JL', 'AU', 'SE', 'OC', 'NO', 'DE']
    
    # Count months with private coverage
    prv_cols = [f'PRVEV{m}15' for m in months]
    fyc['MONTHS_PRIVATE'] = (fyc[prv_cols] == 1).sum(axis=1)
    
    # Count months with public coverage
    pub_cols = [f'PUBEV{m}15' for m in months]
    fyc['MONTHS_PUBLIC'] = (fyc[pub_cols] == 1).sum(axis=1)
    
    # Count months with any coverage
    fyc['MONTHS_INSURED'] = 0
    for m in months:
        prv_var = f'PRVEV{m}15'
        pub_var = f'PUBEV{m}15'
        fyc['MONTHS_INSURED'] += ((fyc[prv_var] == 1) | (fyc[pub_var] == 1)).astype(int)
    
    # Create insurance status categories
    fyc['INS_STATUS'] = 'Unknown'
    fyc.loc[fyc['MONTHS_INSURED'] == 12, 'INS_STATUS'] = 'Insured all year'
    fyc.loc[fyc['MONTHS_INSURED'] == 0, 'INS_STATUS'] = 'Uninsured all year'
    fyc.loc[(fyc['MONTHS_INSURED'] > 0) & (fyc['MONTHS_INSURED'] < 12), 'INS_STATUS'] = 'Part-year insured'
    
    print("\nInsurance status distribution (unweighted):")
    print(fyc['INS_STATUS'].value_counts())
    
    # Define survey design
    design = MEPSSurveyDesign(
        data=fyc,
        strata='VARSTR',
        cluster='VARPSU',
        weights='PERWT15F'
    )
    
    # Proportion by insurance status
    print("\n" + "-" * 60)
    print("PROPORTION BY INSURANCE STATUS")
    print("-" * 60)
    
    for status in ['Insured all year', 'Part-year insured', 'Uninsured all year']:
        fyc[f'is_{status}'] = (fyc['INS_STATUS'] == status).astype(int)
        result = design.mean(f'is_{status}')
        print(f"{status:20s}: {result['mean'].values[0]:.4f} (SE: {result['se'].values[0]:.5f})")
    
    # Mean months insured
    print("\n" + "-" * 60)
    print("MEAN MONTHS INSURED")
    print("-" * 60)
    
    mean_months = design.mean('MONTHS_INSURED')
    print(f"Mean: {mean_months['mean'].values[0]:.2f} months")
    print(f"SE: {mean_months['se'].values[0]:.3f}")
    
    # Mean expenditures by insurance status
    print("\n" + "-" * 60)
    print("MEAN EXPENDITURES BY INSURANCE STATUS")
    print("-" * 60)
    
    mean_by_status = design.mean('TOTEXP15', domain='INS_STATUS')
    for _, row in mean_by_status.iterrows():
        print(f"{row['domain_value']:20s}: ${row['mean']:>10,.2f}  (SE: ${row['se']:>8,.2f})")


if __name__ == "__main__":
    main()
