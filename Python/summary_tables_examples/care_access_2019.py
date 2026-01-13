"""
MEPS Summary Tables Example: Did Not Receive Treatment Because Couldn't Afford It, 2019

This code replicates estimates from the MEPS-HC Data Tools summary tables for
Accessibility and Quality of Care: Access to Care, 2019

Estimates:
  - Number/percent of people who did not receive treatment because couldn't afford it
  - By poverty status

Input file: C:/MEPS/h216.sas7bdat (2019 Full-Year Consolidated file)

This is the Python equivalent of care_access_2019.sas
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
    fyc = load_meps_data("C:/MEPS/h216.sas7bdat")
    
    print("=" * 70)
    print("DID NOT RECEIVE TREATMENT BECAUSE COULDN'T AFFORD IT, 2019")
    print("=" * 70)
    
    # Define variables for affordability
    # AFRDCA42: Couldn't afford medical care
    # AFRDDN42: Couldn't afford dental care
    # AFRDPM42: Couldn't afford prescribed medicines
    
    # Create indicator variables
    fyc['afford_MD'] = (fyc['AFRDCA42'] == 1).astype(int)
    fyc['afford_DN'] = (fyc['AFRDDN42'] == 1).astype(int)
    fyc['afford_PM'] = (fyc['AFRDPM42'] == 1).astype(int)
    fyc['afford_ANY'] = (
        fyc['afford_MD'] | fyc['afford_DN'] | fyc['afford_PM']
    ).astype(int)
    
    # Define domain: persons eligible for access to care supplement
    fyc['domain'] = (fyc['ACCELI42'] == 1).astype(int)
    
    # Adjust weights for domain analysis
    fyc.loc[(fyc['domain'] == 0) & (fyc['PERWT19F'] == 0), 'PERWT19F'] = 1
    
    # Poverty status labels
    pov_labels = {
        1: 'Negative or poor',
        2: 'Near-poor',
        3: 'Low income',
        4: 'Middle income',
        5: 'High income'
    }
    fyc['POV_LABEL'] = fyc['POVCAT19'].map(pov_labels)
    
    # QC new variables
    print("\nQC: Affordability variables")
    print(pd.crosstab(fyc['AFRDCA42'], fyc['afford_MD']))
    
    # Define survey design
    design = MEPSSurveyDesign(
        data=fyc,
        strata='VARSTR',
        cluster='VARPSU',
        weights='PERWT19F'
    )
    
    # Subset to eligible population
    design_domain = design.subset(fyc['domain'] == 1)
    
    # Overall proportions
    print("\n" + "-" * 70)
    print("PROPORTION WHO COULDN'T AFFORD CARE (Overall)")
    print("-" * 70)
    
    afford_vars = {
        'afford_ANY': 'Any care',
        'afford_MD': 'Medical care',
        'afford_DN': 'Dental care',
        'afford_PM': 'Prescribed medicines'
    }
    
    for var, label in afford_vars.items():
        result_mean = design_domain.mean(var)
        result_total = design_domain.total(var)
        print(f"\n{label}:")
        print(f"  Number: {result_total['total'].values[0]:,.0f} (SE: {result_total['se'].values[0]:,.0f})")
        print(f"  Percent: {result_mean['mean'].values[0]:.4f} (SE: {result_mean['se'].values[0]:.5f})")
    
    # By poverty status
    print("\n" + "-" * 70)
    print("COULDN'T AFFORD ANY CARE BY POVERTY STATUS")
    print("-" * 70)
    
    by_pov_mean = design_domain.mean('afford_ANY', domain='POV_LABEL')
    by_pov_total = design_domain.total('afford_ANY', domain='POV_LABEL')
    
    for _, row in by_pov_mean.iterrows():
        total_row = by_pov_total[by_pov_total['domain_value'] == row['domain_value']].iloc[0]
        print(f"\n{row['domain_value']}:")
        print(f"  Number: {total_row['total']:,.0f} (SE: {total_row['se']:,.0f})")
        print(f"  Percent: {row['mean']:.4f} (SE: {row['se']:.5f})")


if __name__ == "__main__":
    main()
