"""
MEPS Summary Tables Example: Prescribed Medicines by Therapeutic Class, 2018

This code replicates estimates from the MEPS-HC Data Tools summary tables for
Prescribed Drugs: Therapeutic Class, 2018

Estimates:
  - Total purchases and expenditures by therapeutic class
  - Top therapeutic classes

Input file: C:/MEPS/h206a.dta (2018 Prescribed Medicines file)
           C:/MEPS/h209.dta (2018 Full-Year Consolidated file)

This is the Python equivalent of pmed_therapeutic_class_2018.sas
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
    pmed = load_meps_data("C:/MEPS/h206a.dta")
    fyc = load_meps_data("C:/MEPS/h209.dta")
    
    print("=" * 70)
    print("PRESCRIBED MEDICINES BY THERAPEUTIC CLASS, 2018")
    print("=" * 70)
    
    # Keep needed variables from PMED file
    # TC1S1_1 is the first-level therapeutic class code
    pmed_sub = pmed[['DUPERSID', 'RXRECIDX', 'TC1S1_1', 'RXXP18X']].copy()
    
    # Keep needed variables from FYC file
    fyc_sub = fyc[['DUPERSID', 'VARSTR', 'VARPSU', 'PERWT18F']].copy()
    
    # Therapeutic class labels (selected common classes)
    tc_labels = {
        40: 'Cardiovascular agents',
        57: 'Central nervous system agents',
        97: 'Hormones/hormone modifiers',
        113: 'Metabolic agents',
        122: 'Respiratory agents',
        133: 'Topical agents',
        193: 'Psychotherapeutic agents',
        242: 'Antipsychotics',
        249: 'Antidepressants',
        60: 'Analgesics',
        191: 'Narcotic analgesics'
    }
    
    # Create count variable for each fill
    pmed_sub['n_fills'] = 1
    
    # Aggregate to person-therapeutic class level
    person_tc = pmed_sub.groupby(['DUPERSID', 'TC1S1_1']).agg({
        'n_fills': 'sum',
        'RXXP18X': 'sum'
    }).reset_index()
    
    # Merge with FYC for survey design variables
    person_tc = person_tc.merge(fyc_sub, on='DUPERSID', how='left')
    
    # Create person-level indicator
    person_tc['person'] = 1
    
    # Add therapeutic class labels
    person_tc['TC_LABEL'] = person_tc['TC1S1_1'].map(tc_labels)
    person_tc['TC_LABEL'] = person_tc['TC_LABEL'].fillna('Other')
    
    print(f"\nTotal person-TC records: {len(person_tc):,}")
    print(f"Unique therapeutic classes: {person_tc['TC1S1_1'].nunique():,}")
    
    # Define survey design
    design = MEPSSurveyDesign(
        data=person_tc,
        strata='VARSTR',
        cluster='VARPSU',
        weights='PERWT18F'
    )
    
    # Get top therapeutic classes by unweighted fills
    top_tc = pmed_sub.groupby('TC1S1_1')['n_fills'].sum().nlargest(10).index.tolist()
    
    print("\n" + "-" * 70)
    print("TOP 10 THERAPEUTIC CLASSES BY NUMBER OF PURCHASES")
    print("-" * 70)
    
    for tc in top_tc:
        tc_data = person_tc[person_tc['TC1S1_1'] == tc]
        tc_design = MEPSSurveyDesign(
            data=tc_data,
            strata='VARSTR',
            cluster='VARPSU',
            weights='PERWT18F'
        )
        
        # Number of people
        n_people = tc_design.total('person')
        # Number of fills
        n_fills = tc_design.total('n_fills')
        # Total expenditures
        total_exp = tc_design.total('RXXP18X')
        
        label = tc_labels.get(tc, f'TC Code {tc}')
        print(f"\n{label} (TC={tc}):")
        print(f"  People: {n_people['total'].values[0]:,.0f}")
        print(f"  Fills: {n_fills['total'].values[0]:,.0f}")
        print(f"  Expenditures: ${total_exp['total'].values[0]:,.0f}")


if __name__ == "__main__":
    main()
