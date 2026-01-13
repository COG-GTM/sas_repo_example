"""
MEPS Workshop: Office-Based Visits for Mental Health, 2020

This example code shows how to link the MEPS-HC Medical Conditions file to the 
Office-Based Medical Provider Visits file for data year 2020 in order to estimate
utilization and expenditures for office-based visits for mental health conditions.

Input files:
  - C:/MEPS/h220g.sas7bdat (2020 Office-Based Medical Provider Visits file)
  - C:/MEPS/h222.sas7bdat (2020 Conditions file)
  - C:/MEPS/h220if1.sas7bdat (2020 CLNK: Condition-Event Link file)
  - C:/MEPS/h224.sas7bdat (2020 Full-Year Consolidated file)

This is the Python equivalent of the SAS program cond_mv_2020.sas
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
    print("OFFICE-BASED VISITS FOR MENTAL HEALTH, 2020")
    print("=" * 70)
    
    # Read in data files
    print("\nLoading data files...")
    ob = load_meps_data("C:/MEPS/h220g.sas7bdat")  # 2020 Office-Based
    cond = load_meps_data("C:/MEPS/h222.sas7bdat")  # 2020 Conditions
    clnk = load_meps_data("C:/MEPS/h220if1.sas7bdat")  # 2020 CLNK
    fyc = load_meps_data("C:/MEPS/h224.sas7bdat")  # 2020 FYC
    
    # Prepare Office-Based file
    ob_sub = ob[['DUPERSID', 'EVNTIDX', 'OBXP20X', 'OBDATEYR', 'OBDATEMM',
                 'VARSTR', 'VARPSU', 'PERWT20F']].copy()
    
    # Prepare Conditions file
    cond_sub = cond[['DUPERSID', 'CONDIDX', 'ICD10CDX',
                     'CCSR1X', 'CCSR2X', 'CCSR3X']].copy()
    
    # Prepare FYC file
    fyc_sub = fyc[['DUPERSID', 'AGELAST', 'SEX', 'RACETHX',
                   'PERWT20F', 'VARPSU', 'VARSTR']].copy()
    
    # CCSR codes for mental health conditions
    # MBD = Mental, Behavioral, and Neurodevelopmental Disorders
    # Common mental health CCSR codes start with 'MBD'
    MH_CCSR_PREFIX = 'MBD'
    
    # Subset conditions to mental health
    mh_cond = cond_sub[
        (cond_sub['CCSR1X'].str.startswith(MH_CCSR_PREFIX, na=False)) |
        (cond_sub['CCSR2X'].str.startswith(MH_CCSR_PREFIX, na=False)) |
        (cond_sub['CCSR3X'].str.startswith(MH_CCSR_PREFIX, na=False))
    ].copy()
    
    print(f"\nNumber of mental health condition records: {len(mh_cond):,}")
    print(f"Unique persons with MH conditions: {mh_cond['DUPERSID'].nunique():,}")
    
    # Get EVNTIDX values for mental health records from CLNK file
    clnk_mh = mh_cond.merge(
        clnk[['DUPERSID', 'CONDIDX', 'EVNTIDX', 'EVENTYPE']],
        on=['DUPERSID', 'CONDIDX'],
        how='inner'
    )
    
    print(f"Events linked to mental health: {len(clnk_mh):,}")
    
    # De-duplicate by EVNTIDX to avoid double-counting
    clnk_mh_dedup = clnk_mh.drop_duplicates(subset=['DUPERSID', 'EVNTIDX'])
    print(f"After de-duplication: {len(clnk_mh_dedup):,}")
    
    # Check event types
    print("\nEvent types linked to mental health:")
    print(clnk_mh_dedup['EVENTYPE'].value_counts())
    
    # Get Office-Based events linked to mental health
    # EVENTYPE = 1 for Office-Based events
    mh_ob = clnk_mh_dedup[clnk_mh_dedup['EVENTYPE'] == 1].merge(
        ob_sub,
        on=['DUPERSID', 'EVNTIDX'],
        how='inner'
    )
    
    print(f"\nOffice-based visits linked to mental health: {len(mh_ob):,}")
    
    # Create dummy variable for each visit
    mh_ob['mh_visit'] = 1
    
    # Roll up to person level
    person_mh = mh_ob.groupby('DUPERSID').agg({
        'mh_visit': 'sum',
        'OBXP20X': 'sum'
    }).reset_index()
    person_mh.columns = ['DUPERSID', 'n_mh_visits', 'mh_ob_exp']
    
    # Merge back to FYC
    fyc_mh = fyc_sub.merge(person_mh, on='DUPERSID', how='left')
    
    # Create flag for persons with any OB visits for mental health
    fyc_mh['mh_ob_flag'] = (fyc_mh['n_mh_visits'] > 0).astype(int)
    
    # Set missing values to zero
    fyc_mh['n_mh_visits'] = fyc_mh['n_mh_visits'].fillna(0)
    fyc_mh['mh_ob_exp'] = fyc_mh['mh_ob_exp'].fillna(0)
    
    # Define survey design
    design = MEPSSurveyDesign(
        data=fyc_mh,
        strata='VARSTR',
        cluster='VARPSU',
        weights='PERWT20F'
    )
    
    # NATIONAL TOTALS
    print("\n" + "=" * 70)
    print("NATIONAL TOTALS")
    print("=" * 70)
    
    # Total people with any OB visits for mental health
    print("\nTotal people with any OB visits for mental health:")
    n_people = design.total('mh_ob_flag')
    print(f"  Total: {n_people['total'].values[0]:,.0f}")
    print(f"  SE: {n_people['se'].values[0]:,.0f}")
    
    # Total number of OB visits for mental health
    print("\nTotal OB visits for mental health:")
    n_visits = design.total('n_mh_visits')
    print(f"  Total: {n_visits['total'].values[0]:,.0f}")
    print(f"  SE: {n_visits['se'].values[0]:,.0f}")
    
    # Total OB expenditures for mental health
    print("\nTotal OB expenditures for mental health:")
    total_exp = design.total('mh_ob_exp')
    print(f"  Total: ${total_exp['total'].values[0]:,.0f}")
    print(f"  SE: ${total_exp['se'].values[0]:,.0f}")
    
    # PER-PERSON AVERAGES
    print("\n" + "=" * 70)
    print("PER-PERSON AVERAGES (among people with any OB visits for MH)")
    print("=" * 70)
    
    # Subset to persons with MH OB visits
    design_mh = design.subset(fyc_mh['mh_ob_flag'] == 1)
    
    # Overall averages
    print("\nOverall:")
    mean_visits = design_mh.mean('n_mh_visits')
    mean_exp = design_mh.mean('mh_ob_exp')
    print(f"  Avg visits: {mean_visits['mean'].values[0]:.2f} (SE: {mean_visits['se'].values[0]:.3f})")
    print(f"  Avg expenditure: ${mean_exp['mean'].values[0]:,.2f} (SE: ${mean_exp['se'].values[0]:,.2f})")
    
    # By sex
    sex_labels = {1: 'Male', 2: 'Female'}
    fyc_mh['SEX_LABEL'] = fyc_mh['SEX'].map(sex_labels)
    design_mh = MEPSSurveyDesign(
        data=fyc_mh[fyc_mh['mh_ob_flag'] == 1],
        strata='VARSTR',
        cluster='VARPSU',
        weights='PERWT20F'
    )
    
    print("\nBy Sex:")
    visits_by_sex = design_mh.mean('n_mh_visits', domain='SEX_LABEL')
    exp_by_sex = design_mh.mean('mh_ob_exp', domain='SEX_LABEL')
    for _, row in visits_by_sex.iterrows():
        exp_row = exp_by_sex[exp_by_sex['domain_value'] == row['domain_value']].iloc[0]
        print(f"  {row['domain_value']:10s}: {row['mean']:.2f} visits, ${exp_row['mean']:,.2f} exp")
    
    # By race/ethnicity
    race_labels = {1: 'Hispanic', 2: 'NH White', 3: 'NH Black',
                   4: 'NH Asian', 5: 'NH Other'}
    fyc_mh['RACE_LABEL'] = fyc_mh['RACETHX'].map(race_labels)
    design_mh = MEPSSurveyDesign(
        data=fyc_mh[fyc_mh['mh_ob_flag'] == 1],
        strata='VARSTR',
        cluster='VARPSU',
        weights='PERWT20F'
    )
    
    print("\nBy Race/Ethnicity:")
    visits_by_race = design_mh.mean('n_mh_visits', domain='RACE_LABEL')
    exp_by_race = design_mh.mean('mh_ob_exp', domain='RACE_LABEL')
    for _, row in visits_by_race.iterrows():
        exp_row = exp_by_race[exp_by_race['domain_value'] == row['domain_value']].iloc[0]
        print(f"  {row['domain_value']:15s}: {row['mean']:.2f} visits, ${exp_row['mean']:,.2f} exp")


if __name__ == "__main__":
    main()
