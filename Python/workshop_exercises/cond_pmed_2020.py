"""
MEPS Workshop: Prescribed Medicine Utilization and Expenditures for Hyperlipidemia, 2020

This example code shows how to link the MEPS-HC Medical Conditions file to the 
Prescribed Medicines file for data year 2020 in order to estimate:

National totals:
   - Total number of people with at least one PMED fill for hyperlipidemia (HL)
   - Total PMED fills for HL
   - Total PMED expenditures for HL 

Per-person averages among people with at least one PMED fill for HL:
   - Avg PMED fills for HL, by sex and poverty (POVCAT20)
   - Avg PMED expenditures for HL, by sex and poverty (POVCAT20)

Input files:
  - C:/MEPS/h220a.sas7bdat (2020 Prescribed Medicines file)
  - C:/MEPS/h222.sas7bdat (2020 Conditions file)
  - C:/MEPS/h220if1.sas7bdat (2020 CLNK: Condition-Event Link file)
  - C:/MEPS/h224.sas7bdat (2020 Full-Year Consolidated file)

This is the Python equivalent of the SAS program cond_pmed_2020.sas
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.meps_loader import load_meps_data
from utils.survey_design import MEPSSurveyDesign


def main():
    print("=" * 70)
    print("PRESCRIBED MEDICINE UTILIZATION FOR HYPERLIPIDEMIA, 2020")
    print("=" * 70)
    
    # Read in data files
    print("\nLoading data files...")
    pmed = load_meps_data("C:/MEPS/h220a.sas7bdat")  # 2020 PMED
    cond = load_meps_data("C:/MEPS/h222.sas7bdat")  # 2020 Conditions
    clnk = load_meps_data("C:/MEPS/h220if1.sas7bdat")  # 2020 CLNK
    fyc = load_meps_data("C:/MEPS/h224.sas7bdat")  # 2020 FYC
    
    # Prepare PMED file
    # Rename LINKIDX to EVNTIDX for merging to conditions
    pmed_sub = pmed[['DUPERSID', 'DRUGIDX', 'RXRECIDX', 'LINKIDX', 
                     'RXDRGNAM', 'RXXP20X']].copy()
    pmed_sub = pmed_sub.rename(columns={'LINKIDX': 'EVNTIDX'})
    
    # Prepare Conditions file
    cond_sub = cond[['DUPERSID', 'CONDIDX', 'ICD10CDX', 
                     'CCSR1X', 'CCSR2X', 'CCSR3X']].copy()
    
    # Prepare FYC file
    fyc_sub = fyc[['DUPERSID', 'AGELAST', 'SEX', 'POVCAT20', 
                   'CHOLDX', 'PERWT20F', 'VARPSU', 'VARSTR']].copy()
    
    # CCSR code for hyperlipidemia: END010
    HL_CCSR = 'END010'
    
    # Subset conditions to hyperlipidemia
    hl_cond = cond_sub[
        (cond_sub['CCSR1X'] == HL_CCSR) | 
        (cond_sub['CCSR2X'] == HL_CCSR) | 
        (cond_sub['CCSR3X'] == HL_CCSR)
    ].copy()
    
    print(f"\nNumber of hyperlipidemia condition records: {len(hl_cond):,}")
    
    # Check for duplicate hyperlipidemia conditions per person
    dup_check = hl_cond.groupby('DUPERSID').size()
    n_dups = (dup_check > 1).sum()
    print(f"Persons with multiple HL condition records: {n_dups:,}")
    
    # Get EVNTIDX values for hyperlipidemia records from CLNK file
    clnk_hl = hl_cond.merge(
        clnk[['DUPERSID', 'CONDIDX', 'EVNTIDX', 'EVENTYPE']],
        on=['DUPERSID', 'CONDIDX'],
        how='inner'
    )
    
    print(f"Events linked to hyperlipidemia: {len(clnk_hl):,}")
    
    # De-duplicate by EVNTIDX to avoid double-counting
    clnk_hl_dedup = clnk_hl.drop_duplicates(subset=['DUPERSID', 'EVNTIDX'])
    print(f"After de-duplication: {len(clnk_hl_dedup):,}")
    
    # Check event types
    print("\nEvent types linked to hyperlipidemia:")
    print(clnk_hl_dedup['EVENTYPE'].value_counts())
    
    # Get PMED events linked to hyperlipidemia
    # EVENTYPE = 8 for PMED events
    hl_pmed = clnk_hl_dedup.merge(
        pmed_sub,
        on=['DUPERSID', 'EVNTIDX'],
        how='inner'
    )
    
    print(f"\nPMED fills linked to hyperlipidemia: {len(hl_pmed):,}")
    
    # QC: Top drugs for hyperlipidemia
    print("\nTop 10 drugs for hyperlipidemia (unweighted):")
    print(hl_pmed['RXDRGNAM'].value_counts().head(10))
    
    # Create dummy variable for each fill
    hl_pmed['hl_fill'] = 1
    
    # Roll up to person level
    person_hl = hl_pmed.groupby('DUPERSID').agg({
        'hl_fill': 'sum',
        'RXXP20X': 'sum'
    }).reset_index()
    person_hl.columns = ['DUPERSID', 'n_hl_fills', 'hl_drug_exp']
    
    # Merge back to FYC
    fyc_hl = fyc_sub.merge(person_hl, on='DUPERSID', how='left')
    
    # Create flag for persons with any PMED fills for hyperlipidemia
    fyc_hl['hl_pmed_flag'] = (fyc_hl['n_hl_fills'] > 0).astype(int)
    
    # Set missing values to zero
    fyc_hl['n_hl_fills'] = fyc_hl['n_hl_fills'].fillna(0)
    fyc_hl['hl_drug_exp'] = fyc_hl['hl_drug_exp'].fillna(0)
    
    # QC: Compare with CHOLDX (ever diagnosed with high cholesterol)
    print("\n" + "-" * 70)
    print("QC: CHOLDX (ever diagnosed) vs HL_PMED_FLAG (has 2020 Rx fills)")
    print(pd.crosstab(fyc_hl['CHOLDX'], fyc_hl['hl_pmed_flag'], margins=True))
    
    # Define survey design
    design = MEPSSurveyDesign(
        data=fyc_hl,
        strata='VARSTR',
        cluster='VARPSU',
        weights='PERWT20F'
    )
    
    # NATIONAL TOTALS
    print("\n" + "=" * 70)
    print("NATIONAL TOTALS")
    print("=" * 70)
    
    # Total people with any Rx fills for hyperlipidemia
    print("\nTotal people with any PMED fills for hyperlipidemia:")
    n_people = design.total('hl_pmed_flag')
    print(f"  Total: {n_people['total'].values[0]:,.0f}")
    print(f"  SE: {n_people['se'].values[0]:,.0f}")
    
    # Total number of Rx fills for hyperlipidemia
    print("\nTotal PMED fills for hyperlipidemia:")
    n_fills = design.total('n_hl_fills')
    print(f"  Total: {n_fills['total'].values[0]:,.0f}")
    print(f"  SE: {n_fills['se'].values[0]:,.0f}")
    
    # Total Rx expenditures for hyperlipidemia
    print("\nTotal PMED expenditures for hyperlipidemia:")
    total_exp = design.total('hl_drug_exp')
    print(f"  Total: ${total_exp['total'].values[0]:,.0f}")
    print(f"  SE: ${total_exp['se'].values[0]:,.0f}")
    
    # PER-PERSON AVERAGES
    print("\n" + "=" * 70)
    print("PER-PERSON AVERAGES (among people with any PMED fills for HL)")
    print("=" * 70)
    
    # Subset to persons with HL PMED fills
    design_hl = design.subset(fyc_hl['hl_pmed_flag'] == 1)
    
    # Overall averages
    print("\nOverall:")
    mean_fills = design_hl.mean('n_hl_fills')
    mean_exp = design_hl.mean('hl_drug_exp')
    print(f"  Avg fills: {mean_fills['mean'].values[0]:.2f} (SE: {mean_fills['se'].values[0]:.3f})")
    print(f"  Avg expenditure: ${mean_exp['mean'].values[0]:,.2f} (SE: ${mean_exp['se'].values[0]:,.2f})")
    
    # By sex
    sex_labels = {1: 'Male', 2: 'Female'}
    fyc_hl['SEX_LABEL'] = fyc_hl['SEX'].map(sex_labels)
    design_hl = MEPSSurveyDesign(
        data=fyc_hl[fyc_hl['hl_pmed_flag'] == 1],
        strata='VARSTR',
        cluster='VARPSU',
        weights='PERWT20F'
    )
    
    print("\nBy Sex:")
    fills_by_sex = design_hl.mean('n_hl_fills', domain='SEX_LABEL')
    exp_by_sex = design_hl.mean('hl_drug_exp', domain='SEX_LABEL')
    for _, row in fills_by_sex.iterrows():
        exp_row = exp_by_sex[exp_by_sex['domain_value'] == row['domain_value']].iloc[0]
        print(f"  {row['domain_value']:10s}: {row['mean']:.2f} fills, ${exp_row['mean']:,.2f} exp")
    
    # By poverty status
    pov_labels = {1: 'Poor/Negative', 2: 'Near Poor', 3: 'Low Income',
                  4: 'Middle Income', 5: 'High Income'}
    fyc_hl['POV_LABEL'] = fyc_hl['POVCAT20'].map(pov_labels)
    design_hl = MEPSSurveyDesign(
        data=fyc_hl[fyc_hl['hl_pmed_flag'] == 1],
        strata='VARSTR',
        cluster='VARPSU',
        weights='PERWT20F'
    )
    
    print("\nBy Poverty Status:")
    fills_by_pov = design_hl.mean('n_hl_fills', domain='POV_LABEL')
    exp_by_pov = design_hl.mean('hl_drug_exp', domain='POV_LABEL')
    for _, row in fills_by_pov.iterrows():
        exp_row = exp_by_pov[exp_by_pov['domain_value'] == row['domain_value']].iloc[0]
        print(f"  {row['domain_value']:15s}: {row['mean']:.2f} fills, ${exp_row['mean']:,.2f} exp")


if __name__ == "__main__":
    main()
