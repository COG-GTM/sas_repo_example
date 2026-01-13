"""
Exercise 4d: Pooling MEPS FYC Files, 2017-2019

This program illustrates how to pool MEPS data files from different years.
It highlights one example of a discontinuity that may be encountered when
working with data from before and after the 2018 MEPS CAPI re-design.

The program pools 2017, 2018 and 2019 data and calculates:
    - Percentage of people with Joint Pain / Arthritis (JTPAIN**, ARTHDX)
    - Average expenditures per person, by Joint Pain status (TOTEXP, TOTSLF)
    - Standard errors by specifying common variance structure when pooling data

Input files:
    - 2017 Full-year consolidated file (h201)
    - 2018 Full-year consolidated file (h209)
    - 2019 Full-year consolidated file (h216)
    - 1996-2019 pooled linkage variance estimation file (h36u19)

This is a Python translation of the SAS program Exercise4.sas (exercise_4d)
"""

import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from utils.meps_loader import load_meps_data
from utils.survey_design import MEPSSurveyDesign, create_pooled_design


def main():
    print("=" * 70)
    print("MEPS DATA USERS WORKSHOP")
    print("EXERCISE 4d: POOLING MEPS FYC FILES, 2017-2019")
    print("=" * 70)

    # Define file paths
    data_folder = "C:/MEPS"
    fyc17_path = f"{data_folder}/h201.sas7bdat"
    fyc18_path = f"{data_folder}/h209.sas7bdat"
    fyc19_path = f"{data_folder}/h216.sas7bdat"
    linkage_path = f"{data_folder}/h36u19.sas7bdat"

    print(f"\nLoading data files...")

    try:
        fyc17 = load_meps_data(fyc17_path)
        fyc18 = load_meps_data(fyc18_path)
        fyc19 = load_meps_data(fyc19_path)
        linkage = load_meps_data(linkage_path)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please download the required files from the MEPS website:")
        print("https://meps.ahrq.gov/mepsweb/data_stats/download_data_files.jsp")
        return

    # Define variables to keep for each year
    kept_vars_2017 = ['DUPERSID', 'PANEL', 'VARSTR', 'VARPSU', 'PERWT17F',
                      'AGELAST', 'ARTHDX', 'JTPAIN31', 'TOTEXP17', 'TOTSLF17']
    kept_vars_2018 = ['DUPERSID', 'PANEL', 'VARSTR', 'VARPSU', 'PERWT18F',
                      'AGELAST', 'ARTHDX', 'JTPAIN31_M18', 'TOTEXP18', 'TOTSLF18']
    kept_vars_2019 = ['DUPERSID', 'PANEL', 'VARSTR', 'VARPSU', 'PERWT19F',
                      'AGELAST', 'ARTHDX', 'JTPAIN31_M18', 'TOTEXP19', 'TOTSLF19']

    # Select and rename variables for each year
    print("\n" + "-" * 70)
    print("Step 1: Prepare and standardize variables across years")
    print("-" * 70)

    # 2017
    fyc17_sub = fyc17[[c for c in kept_vars_2017 if c in fyc17.columns]].copy()
    fyc17_sub = fyc17_sub.rename(columns={
        'TOTEXP17': 'TOTEXP',
        'TOTSLF17': 'TOTSLF',
        'PERWT17F': 'PERWT'
    })
    fyc17_sub['YEAR'] = 2017
    fyc17_sub['JTPAIN'] = fyc17_sub.get('JTPAIN31', np.nan)

    # 2018
    fyc18_sub = fyc18[[c for c in kept_vars_2018 if c in fyc18.columns]].copy()
    fyc18_sub = fyc18_sub.rename(columns={
        'TOTEXP18': 'TOTEXP',
        'TOTSLF18': 'TOTSLF',
        'PERWT18F': 'PERWT',
        'JTPAIN31_M18': 'JTPAIN'
    })
    fyc18_sub['YEAR'] = 2018

    # 2019
    fyc19_sub = fyc19[[c for c in kept_vars_2019 if c in fyc19.columns]].copy()
    fyc19_sub = fyc19_sub.rename(columns={
        'TOTEXP19': 'TOTEXP',
        'TOTSLF19': 'TOTSLF',
        'PERWT19F': 'PERWT',
        'JTPAIN31_M18': 'JTPAIN'
    })
    fyc19_sub['YEAR'] = 2019

    print(f"2017 records: {len(fyc17_sub):,}")
    print(f"2018 records: {len(fyc18_sub):,}")
    print(f"2019 records: {len(fyc19_sub):,}")

    # Concatenate files
    print("\n" + "-" * 70)
    print("Step 2: Concatenate 2017, 2018, and 2019 files")
    print("-" * 70)

    pooled = pd.concat([fyc17_sub, fyc18_sub, fyc19_sub], ignore_index=True)
    print(f"Total pooled records: {len(pooled):,}")

    # Create pooled weight (divide by number of years)
    pooled['POOLWT'] = pooled['PERWT'] / 3

    # Create JOINT_PAIN variable
    # Handle the discontinuity: JTPAIN31 for 2017, JTPAIN31_M18 for 2018-2019
    print("\n" + "-" * 70)
    print("Step 3: Create analysis variables")
    print("-" * 70)

    # Create subpopulation indicator (adults 18+)
    pooled['SPOP'] = 0

    # For 2017
    mask_2017 = (pooled['YEAR'] == 2017) & (pooled['AGELAST'] >= 18)
    mask_2017 &= ~((pooled['ARTHDX'] <= 0) & (pooled['JTPAIN'] < 0))
    pooled.loc[mask_2017, 'SPOP'] = 1

    # For 2018-2019
    mask_1819 = (pooled['YEAR'].isin([2018, 2019])) & (pooled['AGELAST'] >= 18)
    mask_1819 &= ~((pooled['ARTHDX'] < 0) & (pooled['JTPAIN'] < 0))
    pooled.loc[mask_1819, 'SPOP'] = 1

    # Create joint pain indicator
    pooled['JOINT_PAIN'] = np.nan
    mask_jp = pooled['SPOP'] == 1
    pooled.loc[mask_jp, 'JOINT_PAIN'] = np.where(
        (pooled.loc[mask_jp, 'ARTHDX'] == 1) | (pooled.loc[mask_jp, 'JTPAIN'] == 1),
        1, 2
    )

    # Handle DUPERSID format change (8 chars in 2017, 10 chars in 2018+)
    pooled['DUPERSID'] = pooled.apply(
        lambda row: str(int(row['PANEL'])).zfill(2) + str(row['DUPERSID'])
        if row['YEAR'] == 2017 and len(str(row['DUPERSID'])) == 8
        else str(row['DUPERSID']),
        axis=1
    )

    print(f"Adults 18+ (SPOP=1): {(pooled['SPOP'] == 1).sum():,}")
    print(f"With joint pain: {(pooled['JOINT_PAIN'] == 1).sum():,}")
    print(f"Without joint pain: {(pooled['JOINT_PAIN'] == 2).sum():,}")

    # Merge with pooled linkage variance estimation file
    print("\n" + "-" * 70)
    print("Step 4: Merge with Pooled Linkage Variance file")
    print("-" * 70)

    # Prepare linkage file
    linkage_sub = linkage[linkage['PANEL'].isin([21, 22, 23, 24])].copy()

    # Handle DUPERSID format in linkage file
    linkage_sub['DUPERSID'] = linkage_sub.apply(
        lambda row: str(int(row['PANEL'])).zfill(2) + str(row['DUPERSID'])
        if len(str(row['DUPERSID'])) == 8
        else str(row['DUPERSID']),
        axis=1
    )

    linkage_sub = linkage_sub.drop_duplicates(subset=['DUPERSID'])

    # Merge
    pooled_merged = pooled.merge(
        linkage_sub[['DUPERSID', 'STRA9619', 'PSU9619']],
        on='DUPERSID',
        how='left'
    )

    print(f"Records after merge: {len(pooled_merged):,}")
    print(f"Records with variance linkage info: {pooled_merged['STRA9619'].notna().sum():,}")

    # Filter to valid records for analysis
    analysis_data = pooled_merged[
        (pooled_merged['SPOP'] == 1) &
        (pooled_merged['POOLWT'] > 0) &
        (pooled_merged['STRA9619'].notna()) &
        (pooled_merged['PSU9619'].notna())
    ].copy()

    print(f"Records for analysis: {len(analysis_data):,}")

    # Calculate estimates
    print("\n" + "=" * 70)
    print("POOLED ESTIMATES FOR MEPS 2017-2019")
    print("=" * 70)

    # Create survey design using pooled variance structure
    design = create_pooled_design(
        analysis_data,
        years=[2017, 2018, 2019],
        pooled_strata='STRA9619',
        pooled_cluster='PSU9619',
        weight_var='POOLWT'
    )

    # Percentage with joint pain
    print("\n" + "-" * 70)
    print("Percentage of Adults 18+ with Joint Pain/Arthritis")
    print("-" * 70)

    jp_yes = analysis_data[analysis_data['JOINT_PAIN'] == 1].copy()
    jp_no = analysis_data[analysis_data['JOINT_PAIN'] == 2].copy()

    total_pop = analysis_data['POOLWT'].sum()
    jp_yes_pop = jp_yes['POOLWT'].sum()
    jp_no_pop = jp_no['POOLWT'].sum()

    print(f"\n{'Category':20s} {'N':>10s} {'Population':>15s} {'Percent':>10s}")
    print("-" * 60)
    print(f"{'Yes':20s} {len(jp_yes):>10,} {jp_yes_pop:>15,.0f} {jp_yes_pop/total_pop*100:>10.1f}%")
    print(f"{'No':20s} {len(jp_no):>10,} {jp_no_pop:>15,.0f} {jp_no_pop/total_pop*100:>10.1f}%")
    print(f"{'Total':20s} {len(analysis_data):>10,} {total_pop:>15,.0f} {100.0:>10.1f}%")

    # Mean expenditures by joint pain status
    print("\n" + "-" * 70)
    print("Mean Expenditures by Joint Pain Status")
    print("-" * 70)

    print(f"\n{'Joint Pain':15s} {'Variable':15s} {'N':>10s} {'Mean':>12s} {'SE':>10s}")
    print("-" * 65)

    for jp_val, jp_label in [(1, 'Yes'), (2, 'No')]:
        subset = analysis_data[analysis_data['JOINT_PAIN'] == jp_val].copy()
        if len(subset) > 0:
            design_sub = create_pooled_design(
                subset,
                years=[2017, 2018, 2019],
                pooled_strata='STRA9619',
                pooled_cluster='PSU9619',
                weight_var='POOLWT'
            )

            for var in ['TOTEXP', 'TOTSLF']:
                if var in subset.columns:
                    results = design_sub.mean(var)
                    print(f"{jp_label:15s} {var:15s} {results['N'].values[0]:>10,.0f} "
                          f"${results['Mean'].values[0]:>11,.2f} ${results['SE'].values[0]:>9.2f}")

    print("\n" + "=" * 70)
    print("Analysis complete.")
    print("=" * 70)
    print("\nNote: When pooling MEPS data across the 2018 CAPI redesign,")
    print("use the Pooled Linkage Variance file (h36u19) with STRA9619")
    print("and PSU9619 variables for correct standard error estimation.")


if __name__ == "__main__":
    main()
