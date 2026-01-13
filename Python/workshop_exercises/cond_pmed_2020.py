"""
Condition-Event Linking: Prescribed Medicines for Hyperlipidemia, 2020

This program links the MEPS-HC Medical Conditions file to the Prescribed
Medicines file for data year 2020 to estimate:

National totals:
    - Total number of people with at least one PMED fill for hyperlipidemia (HL)
    - Total PMED fills for HL
    - Total PMED expenditures for HL

Per-person averages among people with at least one PMED fill for HL:
    - Avg PMED fills for HL, by sex and poverty (POVCAT20)
    - Avg PMED expenditures for HL, by sex and poverty (POVCAT20)

Input files:
    - h220a.sas7bdat    (2020 Prescribed Medicines file)
    - h222.sas7bdat     (2020 Conditions file)
    - h220if1.sas7bdat  (2020 CLNK: Condition-Event Link file)
    - h224.sas7bdat     (2020 Full-Year Consolidated file)

Resources:
    - CCSR codes: https://github.com/HHS-AHRQ/MEPS/blob/master/Quick_Reference_Guides/meps_ccsr_conditions.csv

This is a Python translation of the SAS program cond_pmed_2020.sas
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
    print("MEPS CONDITION-EVENT LINKING EXAMPLE")
    print("PRESCRIBED MEDICINES FOR HYPERLIPIDEMIA, 2020")
    print("=" * 70)

    # Define file paths
    data_folder = "C:/MEPS"
    pmed_path = f"{data_folder}/h220a.sas7bdat"
    cond_path = f"{data_folder}/h222.sas7bdat"
    clnk_path = f"{data_folder}/h220if1.sas7bdat"
    fyc_path = f"{data_folder}/h224.sas7bdat"

    print("\nLoading data files...")

    try:
        pmed20 = load_meps_data(pmed_path)
        cond20 = load_meps_data(cond_path)
        clnk20 = load_meps_data(clnk_path)
        fyc20 = load_meps_data(fyc_path)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please download the required files from the MEPS website:")
        print("https://meps.ahrq.gov/mepsweb/data_stats/download_data_files.jsp")
        return

    print(f"Prescribed medicines: {len(pmed20):,} records")
    print(f"Conditions: {len(cond20):,} records")
    print(f"CLNK: {len(clnk20):,} records")
    print(f"FYC: {len(fyc20):,} records")

    # Prepare PMED file
    print("\n" + "-" * 70)
    print("Step 1: Prepare prescribed medicines file")
    print("-" * 70)

    # Rename LINKIDX to EVNTIDX for merging
    pmed20x = pmed20.copy()
    if 'LINKIDX' in pmed20x.columns:
        pmed20x['EVNTIDX'] = pmed20x['LINKIDX']

    pmed_vars = ['DUPERSID', 'DRUGIDX', 'RXRECIDX', 'EVNTIDX', 'RXDRGNAM', 'RXXP20X']
    pmed_vars = [v for v in pmed_vars if v in pmed20x.columns]
    pmed20x = pmed20x[pmed_vars].copy()

    print(f"PMED records: {len(pmed20x):,}")

    # Prepare Conditions file
    print("\n" + "-" * 70)
    print("Step 2: Filter conditions to hyperlipidemia (CCSR = END010)")
    print("-" * 70)

    cond_vars = ['DUPERSID', 'CONDIDX', 'ICD10CDX', 'CCSR1X', 'CCSR2X', 'CCSR3X']
    cond_vars = [v for v in cond_vars if v in cond20.columns]
    cond20x = cond20[cond_vars].copy()

    # Filter to hyperlipidemia (CCSR = END010)
    hl_mask = (
        (cond20x['CCSR1X'] == 'END010') |
        (cond20x['CCSR2X'] == 'END010') |
        (cond20x['CCSR3X'] == 'END010')
    )
    hl = cond20x[hl_mask].copy()

    print(f"Hyperlipidemia condition records: {len(hl):,}")

    # Check for duplicate persons with hyperlipidemia
    dup_hl = hl[hl.duplicated(subset=['DUPERSID'], keep=False)]
    print(f"Persons with multiple HL conditions: {dup_hl['DUPERSID'].nunique():,}")

    # Get EVNTIDX values from CLNK file
    print("\n" + "-" * 70)
    print("Step 3: Merge with CLNK to get event IDs")
    print("-" * 70)

    clnk_hl = hl.merge(
        clnk20[['DUPERSID', 'CONDIDX', 'EVNTIDX', 'EVENTYPE']],
        on=['DUPERSID', 'CONDIDX'],
        how='inner'
    )
    print(f"Condition-event links: {len(clnk_hl):,}")

    # De-duplicate by EVNTIDX
    print("\n" + "-" * 70)
    print("Step 4: De-duplicate by event ID (EVNTIDX)")
    print("-" * 70)

    clnk_hl_dedup = clnk_hl.drop_duplicates(subset=['DUPERSID', 'EVNTIDX'])
    print(f"Unique events: {len(clnk_hl_dedup):,}")

    # Check event types
    print(f"\nEvent types in linked data:")
    print(clnk_hl_dedup['EVENTYPE'].value_counts())

    # Merge with PMED file (EVENTYPE = 8 for prescribed medicines)
    print("\n" + "-" * 70)
    print("Step 5: Merge with PMED file")
    print("-" * 70)

    hl_merged = clnk_hl_dedup.merge(
        pmed20x,
        on=['DUPERSID', 'EVNTIDX'],
        how='inner'
    )
    print(f"PMED records linked to hyperlipidemia: {len(hl_merged):,}")

    # QC: Check event types (should all be EVENTYPE = 8)
    print(f"\nEvent types after PMED merge:")
    print(hl_merged['EVENTYPE'].value_counts())

    # QC: Top drugs for hyperlipidemia
    if 'RXDRGNAM' in hl_merged.columns:
        print(f"\nTop 10 drugs for hyperlipidemia (unweighted):")
        print(hl_merged['RXDRGNAM'].value_counts().head(10))

    # Create fill indicator
    hl_merged['HL_FILL'] = 1

    # Aggregate to person level
    print("\n" + "-" * 70)
    print("Step 6: Aggregate to person level")
    print("-" * 70)

    drugs_by_pers = hl_merged.groupby('DUPERSID').agg({
        'HL_FILL': 'sum',
        'RXXP20X': 'sum'
    }).reset_index()

    drugs_by_pers.columns = ['DUPERSID', 'N_HL_FILLS', 'HL_DRUG_EXP']
    print(f"Persons with PMED fills for HL: {len(drugs_by_pers):,}")

    # Merge with FYC
    print("\n" + "-" * 70)
    print("Step 7: Merge with FYC file")
    print("-" * 70)

    fyc_vars = ['DUPERSID', 'AGELAST', 'SEX', 'POVCAT20', 'CHOLDX',
                'PERWT20F', 'VARPSU', 'VARSTR']
    fyc_vars = [v for v in fyc_vars if v in fyc20.columns]
    fyc20x = fyc20[fyc_vars].copy()

    fyc_hl = fyc20x.merge(drugs_by_pers, on='DUPERSID', how='left')

    # Create flag for persons with PMED fills for HL
    fyc_hl['HL_PMED_FLAG'] = np.where(fyc_hl['N_HL_FILLS'] > 0, 1, 0)

    # Fill missing values
    fyc_hl['N_HL_FILLS'] = fyc_hl['N_HL_FILLS'].fillna(0)
    fyc_hl['HL_DRUG_EXP'] = fyc_hl['HL_DRUG_EXP'].fillna(0)

    print(f"Total persons in FYC: {len(fyc_hl):,}")
    print(f"Persons with PMED fills for HL: {(fyc_hl['HL_PMED_FLAG'] == 1).sum():,}")

    # QC: Compare with CHOLDX (ever diagnosed with high cholesterol)
    if 'CHOLDX' in fyc_hl.columns:
        print(f"\nQC: CHOLDX vs HL_PMED_FLAG:")
        print(pd.crosstab(fyc_hl['CHOLDX'], fyc_hl['HL_PMED_FLAG'], margins=True))

    # Define labels
    sex_labels = {1: 'Male', 2: 'Female'}
    povcat_labels = {
        1: 'Negative/Poor',
        2: 'Near Poor',
        3: 'Low Income',
        4: 'Middle Income',
        5: 'High Income'
    }

    # National totals
    print("\n" + "=" * 70)
    print("NATIONAL TOTALS")
    print("=" * 70)

    design = MEPSSurveyDesign(fyc_hl, year=2020)

    # Total people with PMED fills for HL
    pers_total = design.total('HL_PMED_FLAG')
    print(f"\nTotal people with PMED fills for hyperlipidemia:")
    print(f"  Total:  {pers_total['Total'].values[0]:,.0f}")
    print(f"  SE:     {pers_total['SE'].values[0]:,.0f}")

    # Total PMED fills for HL
    fills_total = design.total('N_HL_FILLS')
    print(f"\nTotal PMED fills for hyperlipidemia:")
    print(f"  Total:  {fills_total['Total'].values[0]:,.0f}")
    print(f"  SE:     {fills_total['SE'].values[0]:,.0f}")

    # Total PMED expenditures for HL
    exp_total = design.total('HL_DRUG_EXP')
    print(f"\nTotal PMED expenditures for hyperlipidemia:")
    print(f"  Total:  ${exp_total['Total'].values[0]:,.0f}")
    print(f"  SE:     ${exp_total['SE'].values[0]:,.0f}")

    # Per-person averages
    print("\n" + "=" * 70)
    print("PER-PERSON AVERAGES (Among persons with PMED fills for HL)")
    print("=" * 70)

    # Filter to persons with PMED fills for HL
    fyc_hl_sub = fyc_hl[fyc_hl['HL_PMED_FLAG'] == 1].copy()
    design_sub = MEPSSurveyDesign(fyc_hl_sub, year=2020)

    # Overall
    print("\nOverall:")
    fills_mean = design_sub.mean('N_HL_FILLS')
    exp_mean = design_sub.mean('HL_DRUG_EXP')
    print(f"  Mean fills per person:        {fills_mean['Mean'].values[0]:.2f} (SE: {fills_mean['SE'].values[0]:.4f})")
    print(f"  Mean expenditure per person:  ${exp_mean['Mean'].values[0]:,.2f} (SE: ${exp_mean['SE'].values[0]:.2f})")

    # By sex
    print("\nBy Sex:")
    for sex_val, sex_label in sex_labels.items():
        subset = fyc_hl_sub[fyc_hl_sub['SEX'] == sex_val].copy()
        if len(subset) > 0:
            design_sex = MEPSSurveyDesign(subset, year=2020)
            fills_mean = design_sex.mean('N_HL_FILLS')
            exp_mean = design_sex.mean('HL_DRUG_EXP')
            print(f"  {sex_label}:")
            print(f"    Mean fills:       {fills_mean['Mean'].values[0]:.2f} (SE: {fills_mean['SE'].values[0]:.4f})")
            print(f"    Mean expenditure: ${exp_mean['Mean'].values[0]:,.2f} (SE: ${exp_mean['SE'].values[0]:.2f})")

    # By poverty status
    print("\nBy Poverty Status:")
    for pov_val, pov_label in povcat_labels.items():
        subset = fyc_hl_sub[fyc_hl_sub['POVCAT20'] == pov_val].copy()
        if len(subset) > 0:
            design_pov = MEPSSurveyDesign(subset, year=2020)
            fills_mean = design_pov.mean('N_HL_FILLS')
            exp_mean = design_pov.mean('HL_DRUG_EXP')
            print(f"  {pov_label}:")
            print(f"    Mean fills:       {fills_mean['Mean'].values[0]:.2f} (SE: {fills_mean['SE'].values[0]:.4f})")
            print(f"    Mean expenditure: ${exp_mean['Mean'].values[0]:,.2f} (SE: ${exp_mean['SE'].values[0]:.2f})")

    print("\n" + "=" * 70)
    print("Analysis complete.")
    print("=" * 70)
    print("\nNote: When linking conditions to prescribed medicines:")
    print("  1. Use LINKIDX (renamed to EVNTIDX) in the PMED file")
    print("  2. De-duplicate by EVNTIDX to avoid double-counting")
    print("  3. Merge with FYC for complete variance structure")


if __name__ == "__main__":
    main()
