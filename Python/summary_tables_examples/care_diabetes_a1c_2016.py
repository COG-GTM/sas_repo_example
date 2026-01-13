"""
MEPS Summary Tables Example: Adults with Diabetes Receiving Hemoglobin A1c Test, 2016

This code replicates estimates from the MEPS-HC Data Tools summary tables for
Accessibility and Quality of Care: Diabetes Care, 2016

Estimates:
  - Adults with diabetes receiving hemoglobin A1c blood test
  - By race/ethnicity

Input file: C:/MEPS/h192.ssp (2016 Full-Year Consolidated file)

This is the Python equivalent of care_diabetes_a1c_2016.sas
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
    print("ADULTS WITH DIABETES RECEIVING HEMOGLOBIN A1C TEST, 2016")
    print("=" * 70)
    
    # Define variables
    # DIABDX: Ever been diagnosed with diabetes
    # DSA1C53: Had hemoglobin A1c test in past year
    
    # Create indicator for A1c test
    # DSA1C53: 1 = Yes, 2 = No
    fyc['had_a1c'] = (fyc['DSA1C53'] == 1).astype(int)
    
    # Define domain: adults 18+ with diabetes diagnosis
    fyc['domain'] = (
        (fyc['AGELAST'] >= 18) & 
        (fyc['DIABDX'] == 1)
    ).astype(int)
    
    # Race/ethnicity labels
    race_labels = {
        1: 'Hispanic',
        2: 'NH White',
        3: 'NH Black',
        4: 'NH Asian',
        5: 'NH Other/Multiple'
    }
    fyc['RACE_LABEL'] = fyc['RACETHX'].map(race_labels)
    
    print(f"\nAdults 18+ with diabetes: {fyc['domain'].sum():,}")
    
    # Define survey design
    design = MEPSSurveyDesign(
        data=fyc,
        strata='VARSTR',
        cluster='VARPSU',
        weights='PERWT16F'
    )
    
    # Subset to adults with diabetes
    design_domain = design.subset(fyc['domain'] == 1)
    
    # Overall proportion with A1c test
    print("\n" + "-" * 70)
    print("PROPORTION RECEIVING A1C TEST (Adults with diabetes)")
    print("-" * 70)
    
    result = design_domain.mean('had_a1c')
    print(f"Overall: {result['mean'].values[0]:.4f} (SE: {result['se'].values[0]:.5f})")
    
    # By race/ethnicity
    print("\n" + "-" * 70)
    print("BY RACE/ETHNICITY")
    print("-" * 70)
    
    by_race = design_domain.mean('had_a1c', domain='RACE_LABEL')
    for _, row in by_race.iterrows():
        print(f"{row['domain_value']:20s}: {row['mean']:.4f} (SE: {row['se']:.5f})")


if __name__ == "__main__":
    main()
