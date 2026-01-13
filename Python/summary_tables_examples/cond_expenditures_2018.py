"""
MEPS Summary Tables Example: Medical Conditions and Expenditures, 2018

This code replicates estimates from the MEPS-HC Data Tools summary tables for
Medical Conditions: Expenditures and Utilization, 2018

Estimates:
  - Total expenditures by condition (CCSR category)
  - Number of people with condition

Input file: C:/MEPS/h207.dta (2018 Conditions file)
           C:/MEPS/h209.dta (2018 Full-Year Consolidated file)

This is the Python equivalent of cond_expenditures_2018.sas
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.meps_loader import load_meps_data
from utils.survey_design import MEPSSurveyDesign


def main():
    # Load data files
    cond = load_meps_data("C:/MEPS/h207.dta")
    fyc = load_meps_data("C:/MEPS/h209.dta")
    
    print("=" * 70)
    print("MEDICAL CONDITIONS AND EXPENDITURES, 2018")
    print("=" * 70)
    
    # Keep needed variables from Conditions file
    cond_sub = cond[['DUPERSID', 'CONDIDX', 'CCSR1X', 'CCSR2X', 'CCSR3X']].copy()
    
    # Keep needed variables from FYC file
    fyc_sub = fyc[['DUPERSID', 'TOTEXP18', 'VARSTR', 'VARPSU', 'PERWT18F']].copy()
    
    # CCSR category labels (selected common categories)
    ccsr_labels = {
        'CIR007': 'Essential hypertension',
        'END010': 'Disorders of lipid metabolism',
        'MUS038': 'Osteoarthritis',
        'END002': 'Diabetes mellitus without complication',
        'END003': 'Diabetes mellitus with complication',
        'MBD002': 'Depressive disorders',
        'MBD005': 'Anxiety and fear-related disorders',
        'RSP006': 'Asthma',
        'CIR019': 'Coronary atherosclerosis and other heart disease',
        'NEU011': 'Headache; including migraine'
    }
    
    # Get unique persons with each condition
    # Using CCSR1X as primary condition category
    person_cond = cond_sub.drop_duplicates(subset=['DUPERSID', 'CCSR1X'])
    
    # Merge with FYC
    person_cond = person_cond.merge(fyc_sub, on='DUPERSID', how='left')
    
    print(f"\nTotal person-condition records: {len(person_cond):,}")
    print(f"Unique CCSR categories: {person_cond['CCSR1X'].nunique():,}")
    
    # Analyze selected conditions
    print("\n" + "-" * 70)
    print("SELECTED CONDITIONS: NUMBER OF PEOPLE AND MEAN EXPENDITURE")
    print("-" * 70)
    
    for ccsr, label in ccsr_labels.items():
        # Get persons with this condition
        cond_data = person_cond[person_cond['CCSR1X'] == ccsr].copy()
        
        if len(cond_data) == 0:
            print(f"\n{label} ({ccsr}): No records found")
            continue
        
        # Create person indicator
        cond_data['person'] = 1
        
        # Define survey design
        cond_design = MEPSSurveyDesign(
            data=cond_data,
            strata='VARSTR',
            cluster='VARPSU',
            weights='PERWT18F'
        )
        
        # Number of people
        n_people = cond_design.total('person')
        # Mean total expenditure
        mean_exp = cond_design.mean('TOTEXP18')
        
        print(f"\n{label} ({ccsr}):")
        print(f"  People: {n_people['total'].values[0]:,.0f} (SE: {n_people['se'].values[0]:,.0f})")
        print(f"  Mean expenditure: ${mean_exp['mean'].values[0]:,.2f} (SE: ${mean_exp['se'].values[0]:,.2f})")


if __name__ == "__main__":
    main()
