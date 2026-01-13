"""
MEPS Summary Tables Example: Prescribed Medicines by Drug Name, 2016

This code replicates estimates from the MEPS-HC Data Tools summary tables for
Prescribed Drugs: Prescribed Medicines, 2016

Estimates:
  - Total purchases and expenditures by drug name
  - Top 10 drugs by number of purchases

Input file: C:/MEPS/h188a.ssp (2016 Prescribed Medicines file)
           C:/MEPS/h192.ssp (2016 Full-Year Consolidated file)

This is the Python equivalent of pmed_prescribed_drug_2016.sas
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
    pmed = load_meps_data("C:/MEPS/h188a.ssp")
    fyc = load_meps_data("C:/MEPS/h192.ssp")
    
    print("=" * 70)
    print("PRESCRIBED MEDICINES BY DRUG NAME, 2016")
    print("=" * 70)
    
    # Keep needed variables from PMED file
    pmed_sub = pmed[['DUPERSID', 'RXRECIDX', 'RXDRGNAM', 'RXXP16X']].copy()
    
    # Keep needed variables from FYC file
    fyc_sub = fyc[['DUPERSID', 'VARSTR', 'VARPSU', 'PERWT16F']].copy()
    
    # Create count variable for each fill
    pmed_sub['n_fills'] = 1
    
    # Aggregate to person-drug level
    person_drug = pmed_sub.groupby(['DUPERSID', 'RXDRGNAM']).agg({
        'n_fills': 'sum',
        'RXXP16X': 'sum'
    }).reset_index()
    
    # Merge with FYC for survey design variables
    person_drug = person_drug.merge(fyc_sub, on='DUPERSID', how='left')
    
    # Create person-level indicator
    person_drug['person'] = 1
    
    print(f"\nTotal person-drug records: {len(person_drug):,}")
    print(f"Unique drugs: {person_drug['RXDRGNAM'].nunique():,}")
    
    # Define survey design
    design = MEPSSurveyDesign(
        data=person_drug,
        strata='VARSTR',
        cluster='VARPSU',
        weights='PERWT16F'
    )
    
    # Get top 10 drugs by unweighted fills
    top_drugs = pmed_sub.groupby('RXDRGNAM')['n_fills'].sum().nlargest(10).index.tolist()
    
    print("\n" + "-" * 70)
    print("TOP 10 DRUGS BY NUMBER OF PURCHASES")
    print("-" * 70)
    
    for drug in top_drugs:
        drug_data = person_drug[person_drug['RXDRGNAM'] == drug]
        drug_design = MEPSSurveyDesign(
            data=drug_data,
            strata='VARSTR',
            cluster='VARPSU',
            weights='PERWT16F'
        )
        
        # Number of people
        n_people = drug_design.total('person')
        # Number of fills
        n_fills = drug_design.total('n_fills')
        # Total expenditures
        total_exp = drug_design.total('RXXP16X')
        
        print(f"\n{drug[:40]}:")
        print(f"  People: {n_people['total'].values[0]:,.0f}")
        print(f"  Fills: {n_fills['total'].values[0]:,.0f}")
        print(f"  Expenditures: ${total_exp['total'].values[0]:,.0f}")


if __name__ == "__main__":
    main()
