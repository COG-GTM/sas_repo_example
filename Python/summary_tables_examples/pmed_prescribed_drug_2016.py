"""
Prescribed Medicines Summary Table: Top Drugs by Expenditure, 2016

This program generates estimates on prescribed medicines:
    - Total expenditures by drug name
    - Number of purchases by drug name
    - Number of persons by drug name

Input file:
    - 2016 Prescribed Medicines file (h188a)
    - 2016 Full-year consolidated file (h192)

This is a Python translation of the SAS program pmed_prescribed_drug_2016.sas
"""

import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from utils.meps_loader import load_meps_data
from utils.survey_design import MEPSSurveyDesign


def main():
    print("=" * 70)
    print("MEPS SUMMARY TABLE: TOP DRUGS BY EXPENDITURE, 2016")
    print("=" * 70)

    # Load data files
    pmed_path = "C:/MEPS/h188a.ssp"
    fyc_path = "C:/MEPS/h192.sas7bdat"

    print(f"\nLoading data files...")

    try:
        pmed = load_meps_data(pmed_path)
        fyc = load_meps_data(fyc_path)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please download the required files from the MEPS website:")
        print("https://meps.ahrq.gov/mepsweb/data_stats/download_data_files.jsp")
        return

    print(f"PMED records: {len(pmed):,}")
    print(f"FYC records: {len(fyc):,}")

    # Keep needed variables from PMED
    pmed_vars = ['DUPERSID', 'RXRECIDX', 'RXDRGNAM', 'RXXP16X']
    pmed_vars = [v for v in pmed_vars if v in pmed.columns]
    pmed_sub = pmed[pmed_vars].copy()

    # Keep needed variables from FYC
    fyc_vars = ['DUPERSID', 'VARSTR', 'VARPSU', 'PERWT16F']
    fyc_sub = fyc[fyc_vars].copy()

    # Create purchase indicator
    pmed_sub['N_PURCHASES'] = 1

    # Aggregate to person-drug level
    print("\n" + "-" * 70)
    print("Aggregating to person-drug level")
    print("-" * 70)

    pers_drug = pmed_sub.groupby(['DUPERSID', 'RXDRGNAM']).agg({
        'N_PURCHASES': 'sum',
        'RXXP16X': 'sum'
    }).reset_index()

    pers_drug.columns = ['DUPERSID', 'RXDRGNAM', 'PERS_PURCHASES', 'PERS_EXP']

    # Create person indicator
    pers_drug['PERS_FLAG'] = 1

    print(f"Person-drug records: {len(pers_drug):,}")

    # Merge with FYC for survey design variables
    pers_drug_fyc = pers_drug.merge(fyc_sub, on='DUPERSID', how='left')

    # Filter to valid weights
    pers_drug_fyc = pers_drug_fyc[pers_drug_fyc['PERWT16F'] > 0].copy()

    print(f"Records with valid weights: {len(pers_drug_fyc):,}")

    # Calculate totals by drug name
    print("\n" + "=" * 70)
    print("TOP 20 DRUGS BY TOTAL EXPENDITURE")
    print("=" * 70)

    # Get unique drug names
    drug_names = pers_drug_fyc['RXDRGNAM'].unique()
    print(f"\nUnique drug names: {len(drug_names):,}")

    # Calculate totals for each drug
    drug_totals = []
    for drug in drug_names:
        if pd.isna(drug) or drug == '':
            continue
        
        subset = pers_drug_fyc[pers_drug_fyc['RXDRGNAM'] == drug].copy()
        if len(subset) > 0:
            design = MEPSSurveyDesign(subset, year=2016)
            
            # Total expenditures
            exp_total = design.total('PERS_EXP')
            
            # Total purchases
            purch_total = design.total('PERS_PURCHASES')
            
            # Total persons
            pers_total = design.total('PERS_FLAG')
            
            drug_totals.append({
                'Drug': drug,
                'N_Records': len(subset),
                'Total_Exp': exp_total['Total'].values[0],
                'Total_Purchases': purch_total['Total'].values[0],
                'Total_Persons': pers_total['Total'].values[0]
            })

    # Convert to DataFrame and sort
    drug_df = pd.DataFrame(drug_totals)
    drug_df = drug_df.sort_values('Total_Exp', ascending=False)

    # Print top 20
    print(f"\n{'Rank':>4s} {'Drug Name':40s} {'Total Exp ($M)':>15s} {'Purchases (M)':>15s} {'Persons (M)':>12s}")
    print("-" * 90)

    for i, row in drug_df.head(20).iterrows():
        rank = drug_df.index.get_loc(i) + 1
        print(f"{rank:>4d} {row['Drug'][:40]:40s} ${row['Total_Exp']/1e6:>14,.1f} "
              f"{row['Total_Purchases']/1e6:>14.1f} {row['Total_Persons']/1e6:>11.1f}")

    # Overall totals
    print("\n" + "-" * 70)
    print("Overall Prescribed Medicines Totals")
    print("-" * 70)

    # Aggregate to person level for overall totals
    pers_level = pers_drug_fyc.groupby(['DUPERSID', 'VARSTR', 'VARPSU']).agg({
        'PERWT16F': 'first',
        'PERS_PURCHASES': 'sum',
        'PERS_EXP': 'sum',
        'PERS_FLAG': 'sum'
    }).reset_index()

    pers_level['ANY_PMED'] = (pers_level['PERS_FLAG'] > 0).astype(int)

    design_overall = MEPSSurveyDesign(pers_level, year=2016)

    total_exp = design_overall.total('PERS_EXP')
    total_purch = design_overall.total('PERS_PURCHASES')
    total_pers = design_overall.total('ANY_PMED')

    print(f"\nTotal expenditures: ${total_exp['Total'].values[0]/1e9:,.1f} billion")
    print(f"Total purchases: {total_purch['Total'].values[0]/1e6:,.1f} million")
    print(f"Persons with any PMED: {total_pers['Total'].values[0]/1e6:,.1f} million")

    print("\n" + "=" * 70)
    print("Analysis complete.")
    print("=" * 70)


if __name__ == "__main__":
    main()
