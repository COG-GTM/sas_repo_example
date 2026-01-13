"""
Medical Conditions Summary Table: Expenditures by Condition, 2015

This program generates estimates on healthcare expenditures by medical condition:
    - Total expenditures by condition category
    - Mean expenditure per person with condition
    - Number of persons with condition

Input files:
    - 2015 Full-year consolidated file (h181)
    - 2015 Medical Conditions file (h180)

Note: 2015 uses ICD-9/CCS coding (pre-2016 transition to ICD-10/CCSR)

This is a Python translation of the SAS program cond_expenditures_2015.sas
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
    print("MEPS SUMMARY TABLE: EXPENDITURES BY CONDITION, 2015")
    print("=" * 70)

    # Load data files
    fyc_path = "C:/MEPS/h181.sas7bdat"
    cond_path = "C:/MEPS/h180.sas7bdat"

    print(f"\nLoading data files...")

    try:
        fyc = load_meps_data(fyc_path)
        cond = load_meps_data(cond_path)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please download the required files from the MEPS website:")
        print("https://meps.ahrq.gov/mepsweb/data_stats/download_data_files.jsp")
        return

    print(f"FYC records: {len(fyc):,}")
    print(f"Conditions records: {len(cond):,}")

    # Define CCS condition categories (2015 uses ICD-9/CCS)
    ccs_categories = {
        'Infectious diseases': list(range(1, 10)),
        'Cancer': list(range(11, 45)),
        'Endocrine/metabolic': list(range(48, 59)) + [259],
        'Blood diseases': list(range(59, 65)),
        'Mental disorders': list(range(650, 671)),
        'Nervous system': list(range(76, 96)),
        'Circulatory system': list(range(96, 122)),
        'Respiratory system': list(range(122, 135)),
        'Digestive system': list(range(135, 156)),
        'Genitourinary': list(range(156, 176)),
        'Skin conditions': list(range(197, 212)),
        'Musculoskeletal': list(range(201, 213)),
        'Injury/poisoning': list(range(225, 245)),
    }

    # Keep needed variables from FYC
    fyc_vars = ['DUPERSID', 'VARSTR', 'VARPSU', 'PERWT15F', 'TOTEXP15']
    fyc_vars = [v for v in fyc_vars if v in fyc.columns]
    fyc_sub = fyc[fyc_vars].copy()

    # Keep needed variables from Conditions
    cond_vars = ['DUPERSID', 'CONDIDX', 'CCCODEX', 'ICD9CODX']
    cond_vars = [v for v in cond_vars if v in cond.columns]
    cond_sub = cond[cond_vars].copy()

    # Convert CCCODEX to numeric
    cond_sub['CCS_NUM'] = pd.to_numeric(cond_sub['CCCODEX'], errors='coerce')

    # Create condition category flags
    print("\n" + "-" * 70)
    print("Creating condition category flags")
    print("-" * 70)

    # Get unique persons with each condition category
    for cat_name, ccs_codes in ccs_categories.items():
        cat_persons = cond_sub[cond_sub['CCS_NUM'].isin(ccs_codes)]['DUPERSID'].unique()
        fyc_sub[cat_name.replace('/', '_').replace(' ', '_')] = fyc_sub['DUPERSID'].isin(cat_persons).astype(int)
        print(f"{cat_name}: {len(cat_persons):,} persons")

    # Create survey design
    design = MEPSSurveyDesign(fyc_sub, year=2015)

    # Calculate estimates by condition category
    print("\n" + "=" * 70)
    print("EXPENDITURES BY CONDITION CATEGORY")
    print("=" * 70)

    print(f"\n{'Condition Category':25s} {'N':>10s} {'Total Exp ($M)':>15s} {'Mean Exp':>12s}")
    print("-" * 65)

    for cat_name in ccs_categories.keys():
        col_name = cat_name.replace('/', '_').replace(' ', '_')
        if col_name in fyc_sub.columns:
            # Filter to persons with condition
            subset = fyc_sub[fyc_sub[col_name] == 1].copy()
            if len(subset) > 0:
                design_sub = MEPSSurveyDesign(subset, year=2015)
                
                # Total expenditures
                total_exp = design_sub.total('TOTEXP15')
                total_millions = total_exp['Total'].values[0] / 1e6
                
                # Mean expenditure
                mean_exp = design_sub.mean('TOTEXP15')
                
                print(f"{cat_name:25s} {len(subset):>10,} ${total_millions:>14,.0f} ${mean_exp['Mean'].values[0]:>11,.0f}")

    # Overall totals
    print("\n" + "-" * 70)
    print("Overall Population")
    print("-" * 70)

    total_exp = design.total('TOTEXP15')
    mean_exp = design.mean('TOTEXP15')
    print(f"Total expenditures: ${total_exp['Total'].values[0]/1e9:,.1f} billion")
    print(f"Mean expenditure per person: ${mean_exp['Mean'].values[0]:,.0f}")

    print("\n" + "=" * 70)
    print("Analysis complete.")
    print("=" * 70)
    print("\nNote: 2015 data uses ICD-9/CCS coding system.")
    print("For 2016+ data, use ICD-10/CCSR codes instead.")


if __name__ == "__main__":
    main()
