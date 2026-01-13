"""
MEPS Summary Tables Example: Reasons for Difficulty Receiving Needed Care, 2017

This code replicates estimates from the MEPS-HC Data Tools summary tables for
Accessibility and Quality of Care: Access to Care, 2017

Estimates:
  - Reasons for difficulty receiving needed care
  - By poverty status

Input file: C:/MEPS/h201.dta (2017 Full-Year Consolidated file)

This is the Python equivalent of care_access_2017.sas
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
    fyc = load_meps_data("C:/MEPS/h201.dta")
    
    print("=" * 70)
    print("REASONS FOR DIFFICULTY RECEIVING NEEDED CARE, 2017")
    print("=" * 70)
    
    # Define variables for reasons for difficulty receiving care
    # DTEFMR42: Couldn't get through on phone
    # DTEFAP42: Couldn't get appointment soon enough
    # DTEFWT42: Wait too long in doctor's office
    # DTEFNP42: Not open when needed
    # DTEFNL42: No transportation
    
    # Create indicator variables
    fyc['phone_diff'] = (fyc['DTEFMR42'] == 1).astype(int)
    fyc['appt_diff'] = (fyc['DTEFAP42'] == 1).astype(int)
    fyc['wait_diff'] = (fyc['DTEFWT42'] == 1).astype(int)
    fyc['hours_diff'] = (fyc['DTEFNP42'] == 1).astype(int)
    fyc['transport_diff'] = (fyc['DTEFNL42'] == 1).astype(int)
    
    # Any difficulty
    fyc['any_diff'] = (
        fyc['phone_diff'] | fyc['appt_diff'] | fyc['wait_diff'] |
        fyc['hours_diff'] | fyc['transport_diff']
    ).astype(int)
    
    # Define domain: persons eligible for access to care supplement
    fyc['domain'] = (fyc['ACCESSION'] == 1).astype(int)
    
    # Adjust weights for domain analysis
    fyc.loc[(fyc['domain'] == 0) & (fyc['PERWT17F'] == 0), 'PERWT17F'] = 1
    
    # Poverty status labels
    pov_labels = {
        1: 'Negative or poor',
        2: 'Near-poor',
        3: 'Low income',
        4: 'Middle income',
        5: 'High income'
    }
    fyc['POV_LABEL'] = fyc['POVCAT17'].map(pov_labels)
    
    # Define survey design
    design = MEPSSurveyDesign(
        data=fyc,
        strata='VARSTR',
        cluster='VARPSU',
        weights='PERWT17F'
    )
    
    # Subset to eligible population
    design_domain = design.subset(fyc['domain'] == 1)
    
    # Overall proportions
    print("\n" + "-" * 70)
    print("PROPORTION WITH DIFFICULTY RECEIVING CARE (Overall)")
    print("-" * 70)
    
    diff_vars = {
        'any_diff': 'Any difficulty',
        'phone_diff': "Couldn't get through on phone",
        'appt_diff': "Couldn't get appointment soon enough",
        'wait_diff': 'Wait too long in office',
        'hours_diff': 'Not open when needed',
        'transport_diff': 'No transportation'
    }
    
    for var, label in diff_vars.items():
        result = design_domain.mean(var)
        print(f"{label:40s}: {result['mean'].values[0]:.4f} (SE: {result['se'].values[0]:.5f})")
    
    # By poverty status
    print("\n" + "-" * 70)
    print("ANY DIFFICULTY BY POVERTY STATUS")
    print("-" * 70)
    
    by_pov = design_domain.mean('any_diff', domain='POV_LABEL')
    for _, row in by_pov.iterrows():
        print(f"{row['domain_value']:20s}: {row['mean']:.4f} (SE: {row['se']:.5f})")


if __name__ == "__main__":
    main()
