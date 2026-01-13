"""
Medical Conditions Summary Table: Expenditures by Condition, 2018

This program generates estimates on healthcare expenditures by medical condition:
    - Total expenditures by condition category
    - Mean expenditure per person with condition
    - Number of persons with condition

Input files:
    - 2018 Full-year consolidated file (h209)
    - 2018 Medical Conditions file (h207)

Note: 2018 uses ICD-10/CCSR coding (post-2016 transition)

This is a Python translation of the SAS program cond_expenditures_2018.sas
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
    print("MEPS SUMMARY TABLE: EXPENDITURES BY CONDITION, 2018")
    print("=" * 70)

    # Load data files
    fyc_path = "C:/MEPS/h209.sas7bdat"
    cond_path = "C:/MEPS/h207.sas7bdat"

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

    # Define CCSR condition categories (2018 uses ICD-10/CCSR)
    ccsr_categories = {
        'Infectious diseases': ['INF'],
        'Neoplasms': ['NEO'],
        'Endocrine/metabolic': ['END'],
        'Blood diseases': ['BLD'],
        'Mental disorders': ['MBD'],
        'Nervous system': ['NVS'],
        'Eye disorders': ['EYE'],
        'Ear disorders': ['EAR'],
        'Circulatory system': ['CIR'],
        'Respiratory system': ['RSP'],
        'Digestive system': ['DIG'],
        'Skin conditions': ['SKN'],
        'Musculoskeletal': ['MUS'],
        'Genitourinary': ['GEN'],
        'Pregnancy': ['PRG'],
        'Perinatal': ['PNL'],
        'Congenital': ['MAL'],
        'Symptoms/signs': ['SYM'],
        'Injury/poisoning': ['INJ'],
        'External causes': ['EXT'],
        'Factors influencing': ['FAC'],
    }

    # Keep needed variables from FYC
    fyc_vars = ['DUPERSID', 'VARSTR', 'VARPSU', 'PERWT18F', 'TOTEXP18']
    fyc_vars = [v for v in fyc_vars if v in fyc.columns]
    fyc_sub = fyc[fyc_vars].copy()

    # Keep needed variables from Conditions
    cond_vars = ['DUPERSID', 'CONDIDX', 'CCSR1X', 'CCSR2X', 'CCSR3X', 'ICD10CDX']
    cond_vars = [v for v in cond_vars if v in cond.columns]
    cond_sub = cond[cond_vars].copy()

    # Extract body system prefix from CCSR codes
    cond_sub['BODY_SYS'] = cond_sub['CCSR1X'].str[:3]

    # Create condition category flags
    print("\n" + "-" * 70)
    print("Creating condition category flags")
    print("-" * 70)

    # Get unique persons with each condition category
    for cat_name, prefixes in ccsr_categories.items():
        cat_persons = cond_sub[cond_sub['BODY_SYS'].isin(prefixes)]['DUPERSID'].unique()
        col_name = cat_name.replace('/', '_').replace(' ', '_')
        fyc_sub[col_name] = fyc_sub['DUPERSID'].isin(cat_persons).astype(int)
        print(f"{cat_name}: {len(cat_persons):,} persons")

    # Create survey design
    design = MEPSSurveyDesign(fyc_sub, year=2018)

    # Calculate estimates by condition category
    print("\n" + "=" * 70)
    print("EXPENDITURES BY CONDITION CATEGORY")
    print("=" * 70)

    print(f"\n{'Condition Category':25s} {'N':>10s} {'Total Exp ($M)':>15s} {'Mean Exp':>12s}")
    print("-" * 65)

    for cat_name in ccsr_categories.keys():
        col_name = cat_name.replace('/', '_').replace(' ', '_')
        if col_name in fyc_sub.columns:
            # Filter to persons with condition
            subset = fyc_sub[fyc_sub[col_name] == 1].copy()
            if len(subset) > 0:
                design_sub = MEPSSurveyDesign(subset, year=2018)
                
                # Total expenditures
                total_exp = design_sub.total('TOTEXP18')
                total_millions = total_exp['Total'].values[0] / 1e6
                
                # Mean expenditure
                mean_exp = design_sub.mean('TOTEXP18')
                
                print(f"{cat_name:25s} {len(subset):>10,} ${total_millions:>14,.0f} ${mean_exp['Mean'].values[0]:>11,.0f}")

    # Overall totals
    print("\n" + "-" * 70)
    print("Overall Population")
    print("-" * 70)

    total_exp = design.total('TOTEXP18')
    mean_exp = design.mean('TOTEXP18')
    print(f"Total expenditures: ${total_exp['Total'].values[0]/1e9:,.1f} billion")
    print(f"Mean expenditure per person: ${mean_exp['Mean'].values[0]:,.0f}")

    print("\n" + "=" * 70)
    print("Analysis complete.")
    print("=" * 70)
    print("\nNote: 2018 data uses ICD-10/CCSR coding system.")
    print("Body system prefixes: INF, NEO, END, BLD, MBD, NVS, etc.")


if __name__ == "__main__":
    main()
