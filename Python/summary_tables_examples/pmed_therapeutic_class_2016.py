"""
Prescribed Medicines Summary Table: Expenditures by Therapeutic Class, 2016

This program generates estimates on prescribed medicines by therapeutic class:
    - Total expenditures by therapeutic class
    - Number of purchases by therapeutic class
    - Number of persons by therapeutic class

Input file:
    - 2016 Prescribed Medicines file (h188a)
    - 2016 Full-year consolidated file (h192)

This is a Python translation of the SAS program pmed_therapeutic_class_2016.sas
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
    print("MEPS SUMMARY TABLE: EXPENDITURES BY THERAPEUTIC CLASS, 2016")
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

    # Define therapeutic class labels (TC1)
    tc1_labels = {
        1: 'Anti-infectives',
        19: 'Antineoplastics',
        20: 'Biologicals',
        28: 'Cardiovascular agents',
        40: 'Central nervous system agents',
        57: 'Coagulation modifiers',
        81: 'Gastrointestinal agents',
        87: 'Hormones/hormone modifiers',
        97: 'Immunologic agents',
        105: 'Metabolic agents',
        113: 'Miscellaneous agents',
        115: 'Nutritional products',
        122: 'Respiratory agents',
        133: 'Topical agents',
        218: 'Alternative medicines',
        242: 'Psychotherapeutic agents',
        254: 'Radiologic agents',
        358: 'Genitourinary tract agents',
    }

    # Keep needed variables from PMED
    pmed_vars = ['DUPERSID', 'RXRECIDX', 'TC1', 'TC1S1', 'RXXP16X']
    pmed_vars = [v for v in pmed_vars if v in pmed.columns]
    pmed_sub = pmed[pmed_vars].copy()

    # Keep needed variables from FYC
    fyc_vars = ['DUPERSID', 'VARSTR', 'VARPSU', 'PERWT16F']
    fyc_sub = fyc[fyc_vars].copy()

    # Create purchase indicator
    pmed_sub['N_PURCHASES'] = 1

    # Aggregate to person-TC1 level
    print("\n" + "-" * 70)
    print("Aggregating to person-therapeutic class level")
    print("-" * 70)

    pers_tc = pmed_sub.groupby(['DUPERSID', 'TC1']).agg({
        'N_PURCHASES': 'sum',
        'RXXP16X': 'sum'
    }).reset_index()

    pers_tc.columns = ['DUPERSID', 'TC1', 'PERS_PURCHASES', 'PERS_EXP']

    # Create person indicator
    pers_tc['PERS_FLAG'] = 1

    print(f"Person-TC records: {len(pers_tc):,}")

    # Merge with FYC for survey design variables
    pers_tc_fyc = pers_tc.merge(fyc_sub, on='DUPERSID', how='left')

    # Filter to valid weights
    pers_tc_fyc = pers_tc_fyc[pers_tc_fyc['PERWT16F'] > 0].copy()

    print(f"Records with valid weights: {len(pers_tc_fyc):,}")

    # Calculate totals by therapeutic class
    print("\n" + "=" * 70)
    print("EXPENDITURES BY THERAPEUTIC CLASS")
    print("=" * 70)

    print(f"\n{'TC1':>5s} {'Therapeutic Class':35s} {'Total Exp ($M)':>15s} {'Purchases (M)':>15s} {'Persons (M)':>12s}")
    print("-" * 90)

    tc_totals = []
    for tc1_val, tc1_label in tc1_labels.items():
        subset = pers_tc_fyc[pers_tc_fyc['TC1'] == tc1_val].copy()
        if len(subset) > 0:
            design = MEPSSurveyDesign(subset, year=2016)
            
            # Total expenditures
            exp_total = design.total('PERS_EXP')
            
            # Total purchases
            purch_total = design.total('PERS_PURCHASES')
            
            # Total persons
            pers_total = design.total('PERS_FLAG')
            
            tc_totals.append({
                'TC1': tc1_val,
                'Label': tc1_label,
                'Total_Exp': exp_total['Total'].values[0],
                'Total_Purchases': purch_total['Total'].values[0],
                'Total_Persons': pers_total['Total'].values[0]
            })
            
            print(f"{tc1_val:>5d} {tc1_label:35s} ${exp_total['Total'].values[0]/1e6:>14,.1f} "
                  f"{purch_total['Total'].values[0]/1e6:>14.1f} {pers_total['Total'].values[0]/1e6:>11.1f}")

    # Sort by expenditure
    tc_df = pd.DataFrame(tc_totals)
    tc_df = tc_df.sort_values('Total_Exp', ascending=False)

    print("\n" + "-" * 70)
    print("TOP 10 THERAPEUTIC CLASSES BY EXPENDITURE")
    print("-" * 70)

    print(f"\n{'Rank':>4s} {'Therapeutic Class':35s} {'Total Exp ($M)':>15s} {'% of Total':>12s}")
    print("-" * 70)

    total_all_exp = tc_df['Total_Exp'].sum()
    for i, (_, row) in enumerate(tc_df.head(10).iterrows(), 1):
        pct = row['Total_Exp'] / total_all_exp * 100
        print(f"{i:>4d} {row['Label']:35s} ${row['Total_Exp']/1e6:>14,.1f} {pct:>11.1f}%")

    # Overall totals
    print("\n" + "-" * 70)
    print("Overall Prescribed Medicines Totals")
    print("-" * 70)

    print(f"\nTotal expenditures: ${total_all_exp/1e9:,.1f} billion")
    print(f"Total purchases: {tc_df['Total_Purchases'].sum()/1e6:,.1f} million")

    print("\n" + "=" * 70)
    print("Analysis complete.")
    print("=" * 70)
    print("\nNote: TC1 = Therapeutic Class 1 (Multum Lexicon)")
    print("See MEPS documentation for complete TC1 code list.")


if __name__ == "__main__":
    main()
