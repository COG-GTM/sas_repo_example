"""
Condition-Event Linking: Office-Based Visits for Mental Health, 2020

This program links the MEPS-HC Medical Conditions file to the Office-based
medical visits file for data year 2020 to calculate:

Event-level estimates:
    - Number of office-based visits for mental health
    - Total expenditures for office-based mental health treatment
    - Mean expenditure per office-based mental health visit

Person-level estimates:
    - Number of people with office-based mental health visits
    - Percent of people with office-based mental health visits
    - Mean expenditure per person for office-based mental health visits

Input files:
    - h220g.sas7bdat   (2020 Office-based event file)
    - h222.sas7bdat    (2020 Conditions file)
    - h220if1.sas7bdat (2020 CLNK: Condition-event link file)
    - h224.sas7bdat    (2020 Full-Year Consolidated file)

Resources:
    - CCSR codes: https://github.com/HHS-AHRQ/MEPS/blob/master/Quick_Reference_Guides/meps_ccsr_conditions.csv

This is a Python translation of the SAS program cond_mv_2020.sas
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
    print("OFFICE-BASED VISITS FOR MENTAL HEALTH, 2020")
    print("=" * 70)

    # Define file paths
    data_folder = "C:/MEPS"
    ob_path = f"{data_folder}/h220g.sas7bdat"
    cond_path = f"{data_folder}/h222.sas7bdat"
    clnk_path = f"{data_folder}/h220if1.sas7bdat"
    fyc_path = f"{data_folder}/h224.sas7bdat"

    print("\nLoading data files...")

    try:
        ob20 = load_meps_data(ob_path)
        cond20 = load_meps_data(cond_path)
        clnk20 = load_meps_data(clnk_path)
        fyc20 = load_meps_data(fyc_path)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please download the required files from the MEPS website:")
        print("https://meps.ahrq.gov/mepsweb/data_stats/download_data_files.jsp")
        return

    print(f"Office-based visits: {len(ob20):,} records")
    print(f"Conditions: {len(cond20):,} records")
    print(f"CLNK: {len(clnk20):,} records")
    print(f"FYC: {len(fyc20):,} records")

    # Keep only needed variables
    print("\n" + "-" * 70)
    print("Step 1: Select needed variables")
    print("-" * 70)

    ob_vars = ['PANEL', 'DUPERSID', 'EVNTIDX', 'EVENTRN', 'OBXP20X',
               'PERWT20F', 'VARSTR', 'VARPSU']
    ob_vars = [v for v in ob_vars if v in ob20.columns]
    ob20x = ob20[ob_vars].copy()

    cond_vars = ['DUPERSID', 'CONDIDX', 'ICD10CDX', 'CCSR1X', 'CCSR2X', 'CCSR3X']
    cond_vars = [v for v in cond_vars if v in cond20.columns]
    cond20x = cond20[cond_vars].copy()

    fyc_vars = ['DUPERSID', 'PERWT20F', 'VARSTR', 'VARPSU']
    fyc20x = fyc20[fyc_vars].copy()

    # Filter conditions to Mental Health
    print("\n" + "-" * 70)
    print("Step 2: Filter conditions to Mental Health")
    print("-" * 70)

    # Mental health CCSR codes: MBD*, FAC002, FAC007, NVS011, SYM008, SYM009
    cond20x['ALL_CCSR'] = (cond20x['CCSR1X'].fillna('') +
                           cond20x['CCSR2X'].fillna('') +
                           cond20x['CCSR3X'].fillna(''))

    mental_health_mask = (
        cond20x['ALL_CCSR'].str.contains('MBD', na=False) |
        cond20x['ALL_CCSR'].str.contains('FAC002', na=False) |
        cond20x['ALL_CCSR'].str.contains('FAC007', na=False) |
        cond20x['ALL_CCSR'].str.contains('NVS011', na=False) |
        cond20x['ALL_CCSR'].str.contains('SYM008', na=False) |
        cond20x['ALL_CCSR'].str.contains('SYM009', na=False)
    )

    mental_health = cond20x[mental_health_mask].copy()
    print(f"Mental health condition records: {len(mental_health):,}")

    # Filter CLNK to office-based visits only
    print("\n" + "-" * 70)
    print("Step 3: Filter CLNK to office-based visits (EVENTYPE=1)")
    print("-" * 70)

    # EVENTYPE: 1=Office-based, 2=Outpatient, 3=ER, 4=Inpatient, 7=Home health, 8=PMED
    clnk_ob = clnk20[clnk20['EVENTYPE'] == 1].copy()
    print(f"CLNK office-based records: {len(clnk_ob):,}")

    # Merge conditions with CLNK
    print("\n" + "-" * 70)
    print("Step 4: Merge mental health conditions with CLNK")
    print("-" * 70)

    mh_clnk = mental_health.merge(
        clnk_ob[['DUPERSID', 'CONDIDX', 'EVNTIDX', 'EVENTYPE']],
        on=['DUPERSID', 'CONDIDX'],
        how='inner'
    )
    print(f"Merged records: {len(mh_clnk):,}")

    # De-duplicate by EVNTIDX
    print("\n" + "-" * 70)
    print("Step 5: De-duplicate by event ID (EVNTIDX)")
    print("-" * 70)

    mh_clnk_nodup = mh_clnk[['DUPERSID', 'EVNTIDX', 'EVENTYPE']].drop_duplicates()
    print(f"Unique events: {len(mh_clnk_nodup):,}")

    # Merge with office-based event file
    print("\n" + "-" * 70)
    print("Step 6: Merge with office-based event file")
    print("-" * 70)

    ob_mental_health = mh_clnk_nodup.merge(
        ob20x,
        on=['DUPERSID', 'EVNTIDX'],
        how='inner'
    )
    ob_mental_health['MH_OB_VISIT'] = 1
    print(f"Mental health office-based visits: {len(ob_mental_health):,}")

    # Merge with FYC for complete strata/PSU
    print("\n" + "-" * 70)
    print("Step 7: Merge with FYC for complete variance structure")
    print("-" * 70)

    # Create person-level indicator
    mh_persons = ob_mental_health[['DUPERSID']].drop_duplicates()
    mh_persons['MH_OB'] = 1

    # Merge with FYC
    ob_mh_fyc = fyc20x.merge(mh_persons, on='DUPERSID', how='left')
    ob_mh_fyc['MH_OB'] = ob_mh_fyc['MH_OB'].fillna(0).astype(int)

    # Merge event-level data
    ob_mh_fyc = ob_mh_fyc.merge(
        ob_mental_health[['DUPERSID', 'EVNTIDX', 'OBXP20X', 'MH_OB_VISIT']],
        on='DUPERSID',
        how='left'
    )
    ob_mh_fyc['MH_OB_VISIT'] = ob_mh_fyc['MH_OB_VISIT'].fillna(0).astype(int)
    ob_mh_fyc['OBXP20X'] = ob_mh_fyc['OBXP20X'].fillna(0)

    print(f"Total records after merge: {len(ob_mh_fyc):,}")
    print(f"Persons with MH OB visits: {(ob_mh_fyc['MH_OB'] == 1).sum():,}")

    # Event-level estimates
    print("\n" + "=" * 70)
    print("EVENT-LEVEL ESTIMATES")
    print("=" * 70)

    # Filter to mental health events
    event_data = ob_mh_fyc[ob_mh_fyc['MH_OB'] == 1].copy()

    # Create survey design
    design_event = MEPSSurveyDesign(event_data, year=2020)

    # Number of visits
    visit_total = design_event.total('MH_OB_VISIT')
    print(f"\nNumber of office-based visits for mental health:")
    print(f"  Total:  {visit_total['Total'].values[0]:,.0f}")
    print(f"  SE:     {visit_total['SE'].values[0]:,.0f}")

    # Total expenditures
    exp_total = design_event.total('OBXP20X')
    print(f"\nTotal expenditures for office-based mental health visits:")
    print(f"  Total:  ${exp_total['Total'].values[0]:,.0f}")
    print(f"  SE:     ${exp_total['SE'].values[0]:,.0f}")

    # Mean expenditure per visit
    exp_mean = design_event.mean('OBXP20X')
    print(f"\nMean expenditure per office-based mental health visit:")
    print(f"  Mean:   ${exp_mean['Mean'].values[0]:,.2f}")
    print(f"  SE:     ${exp_mean['SE'].values[0]:.2f}")

    # Person-level estimates
    print("\n" + "=" * 70)
    print("PERSON-LEVEL ESTIMATES")
    print("=" * 70)

    # Aggregate to person level
    pers_mh = ob_mh_fyc.groupby(['DUPERSID', 'VARSTR', 'VARPSU']).agg({
        'PERWT20F': 'first',
        'OBXP20X': 'sum',
        'MH_OB_VISIT': 'sum',
        'MH_OB': 'first'
    }).reset_index()

    pers_mh.columns = ['DUPERSID', 'VARSTR', 'VARPSU', 'PERWT20F',
                       'PERS_XP', 'PERS_NEVENTS', 'MH_OB_PERS']

    # Create indicator for persons with MH visits
    pers_mh['MH_OB_VISIT_PERS'] = (pers_mh['PERS_NEVENTS'] > 0).astype(int)

    print(f"\nPerson-level records: {len(pers_mh):,}")
    print(f"Persons with MH OB visits: {(pers_mh['MH_OB_VISIT_PERS'] == 1).sum():,}")

    # Create survey design for person-level
    design_pers = MEPSSurveyDesign(pers_mh, year=2020)

    # Number of people with MH visits
    pers_total = design_pers.total('MH_OB_VISIT_PERS')
    print(f"\nNumber of people with office-based mental health visits:")
    print(f"  Total:  {pers_total['Total'].values[0]:,.0f}")
    print(f"  SE:     {pers_total['SE'].values[0]:,.0f}")

    # Percent of people with MH visits
    pers_pct = design_pers.mean('MH_OB_VISIT_PERS')
    print(f"\nPercent of people with office-based mental health visits:")
    print(f"  Percent: {pers_pct['Mean'].values[0]*100:.2f}%")
    print(f"  SE:      {pers_pct['SE'].values[0]*100:.2f}%")

    # Mean expenditure per person (among those with visits)
    pers_mh_sub = pers_mh[pers_mh['MH_OB_VISIT_PERS'] == 1].copy()
    if len(pers_mh_sub) > 0:
        design_pers_sub = MEPSSurveyDesign(pers_mh_sub, year=2020)
        pers_exp_mean = design_pers_sub.mean('PERS_XP')
        print(f"\nMean expenditure per person for office-based mental health visits:")
        print(f"  Mean:   ${pers_exp_mean['Mean'].values[0]:,.2f}")
        print(f"  SE:     ${pers_exp_mean['SE'].values[0]:.2f}")

    # QC: Total visits and expenditures
    total_visits = design_pers.total('PERS_NEVENTS')
    total_exp = design_pers.total('PERS_XP')
    print(f"\nQC - Total visits (person-level sum): {total_visits['Total'].values[0]:,.0f}")
    print(f"QC - Total expenditures (person-level sum): ${total_exp['Total'].values[0]:,.0f}")

    print("\n" + "=" * 70)
    print("Analysis complete.")
    print("=" * 70)
    print("\nNote: When linking conditions to events, always:")
    print("  1. Filter conditions by CCSR code")
    print("  2. Filter CLNK by event type")
    print("  3. De-duplicate by EVNTIDX to avoid double-counting")
    print("  4. Merge with FYC for complete variance structure")


if __name__ == "__main__":
    main()
