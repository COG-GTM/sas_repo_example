"""
MEPS Summary Tables Example: Ability to Schedule a Routine Appointment, 2016

This code replicates estimates from the MEPS-HC Data Tools summary tables for
Accessibility and Quality of Care: Quality of Care, 2016

Estimates:
  - Ability to schedule a routine appointment
  - By insurance coverage

Input file: C:/MEPS/h192.ssp (2016 Full-Year Consolidated file)

This is the Python equivalent of care_quality_2016.sas
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
    print("ABILITY TO SCHEDULE A ROUTINE APPOINTMENT, 2016")
    print("=" * 70)
    
    # Define variables
    # APTS4253: Usually/always able to get routine appointment as soon as needed
    # 1 = Never, 2 = Sometimes, 3 = Usually, 4 = Always
    
    # Create indicator for usually/always able to schedule
    fyc['can_schedule'] = (fyc['APTS4253'].isin([3, 4])).astype(int)
    
    # Define domain: persons with valid response
    fyc['domain'] = (fyc['APTS4253'].isin([1, 2, 3, 4])).astype(int)
    
    # Insurance coverage labels
    ins_labels = {
        1: 'Any private',
        2: 'Public only',
        3: 'Uninsured'
    }
    fyc['INS_LABEL'] = fyc['INSCOV16'].map(ins_labels)
    
    print(f"\nPersons with valid response: {fyc['domain'].sum():,}")
    
    # Define survey design
    design = MEPSSurveyDesign(
        data=fyc,
        strata='VARSTR',
        cluster='VARPSU',
        weights='PERWT16F'
    )
    
    # Subset to valid responses
    design_domain = design.subset(fyc['domain'] == 1)
    
    # Overall proportion
    print("\n" + "-" * 70)
    print("PROPORTION USUALLY/ALWAYS ABLE TO SCHEDULE ROUTINE APPOINTMENT")
    print("-" * 70)
    
    result = design_domain.mean('can_schedule')
    print(f"Overall: {result['mean'].values[0]:.4f} (SE: {result['se'].values[0]:.5f})")
    
    # By insurance coverage
    print("\n" + "-" * 70)
    print("BY INSURANCE COVERAGE")
    print("-" * 70)
    
    by_ins = design_domain.mean('can_schedule', domain='INS_LABEL')
    for _, row in by_ins.iterrows():
        print(f"{row['domain_value']:15s}: {row['mean']:.4f} (SE: {row['se']:.5f})")


if __name__ == "__main__":
    main()
