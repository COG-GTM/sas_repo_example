"""
Exercise 2a: Antipsychotics Purchases and Expenses, 2015

This program generates selected estimates for a 2015 version of the
MEPS Statistics Brief #275: "Trends in Antipsychotics Purchases and Expenses
for the U.S. Civilian Noninstitutionalized Population, 1997 and 2007"

Estimates include:
    (1) Total expense for antipsychotics
    (2) Total number of purchases of antipsychotics
    (3) Total number of persons purchasing one or more antipsychotics
    (4) Average total, out of pocket, and third party payer expense
        for antipsychotics per person with an antipsychotic medicine purchase

Input files:
    (1) C:/MEPS/h181.sas7bdat (2015 Full-Year Consolidated PUF)
    (2) C:/MEPS/h178a.sas7bdat (2015 Prescribed Medicines PUF)

This is a Python translation of the SAS program Exercise2a.sas
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
    print("2018 AHRQ MEPS DATA USERS WORKSHOP")
    print("EXERCISE 2a: Antipsychotics Purchases and Expenses, 2015")
    print("=" * 70)

    # Load data files
    fyc_path = "C:/MEPS/h181.sas7bdat"
    pmed_path = "C:/MEPS/h178a.sas7bdat"

    print(f"\nLoading FYC data from: {fyc_path}")
    print(f"Loading PMED data from: {pmed_path}")

    try:
        fyc = load_meps_data(fyc_path)
        pmed = load_meps_data(pmed_path)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please download the required files from the MEPS website:")
        print("https://meps.ahrq.gov/mepsweb/data_stats/download_data_files.jsp")
        return

    # Step 1: Identify antipsychotic drugs using therapeutic classification (TC) codes
    # Definition: TC1=242 AND TC1S1=251
    print("\n" + "-" * 70)
    print("Step 1: Identify antipsychotic drugs")
    print("-" * 70)

    drug = pmed[(pmed['TC1'] == 242) & (pmed['TC1S1'] == 251)].copy()
    print(f"Number of antipsychotic drug records: {len(drug):,}")

    # Sample dump for PMED records with antipsychotic drugs
    print("\nSample PMED records with antipsychotic drugs:")
    sample_cols = ['DUPERSID', 'RXRECIDX', 'LINKIDX', 'TC1', 'TC1S1', 'RXXP15X', 'RXSF15X']
    available_cols = [c for c in sample_cols if c in drug.columns]
    print(drug[available_cols].head(10).to_string())

    # Step 2: Sum data to person-level
    print("\n" + "-" * 70)
    print("Step 2: Aggregate to person-level")
    print("-" * 70)

    perdrug = drug.groupby('DUPERSID').agg({
        'RXXP15X': 'sum',
        'RXSF15X': 'sum',
        'RXRECIDX': 'count'
    }).reset_index()

    perdrug.columns = ['DUPERSID', 'TOT', 'OOP', 'N_PHRCHASE']
    perdrug['THIRD_PAYER'] = perdrug['TOT'] - perdrug['OOP']

    print(f"Number of persons with antipsychotic purchases: {len(perdrug):,}")
    print("\nSample person-level expenditures:")
    print(perdrug.head(10).to_string())

    # Step 3: Merge person-level expenditures to the FY PUF
    print("\n" + "-" * 70)
    print("Step 3: Merge with Full-Year file")
    print("-" * 70)

    # Keep only needed variables from FYC
    fyc_vars = ['DUPERSID', 'VARSTR', 'VARPSU', 'PERWT15F']
    fyc_subset = fyc[fyc_vars].copy()

    # Merge
    fy = fyc_subset.merge(perdrug, on='DUPERSID', how='left')

    # Create subpopulation flag
    fy['SUB'] = np.where(fy['TOT'].notna(), 1, 2)

    # Fill missing values for persons without antipsychotic purchases
    for col in ['N_PHRCHASE', 'TOT', 'OOP', 'THIRD_PAYER']:
        fy[col] = fy[col].fillna(0)

    print(f"Total persons in FYC: {len(fy):,}")
    print(f"Persons with antipsychotic purchases (SUB=1): {(fy['SUB'] == 1).sum():,}")
    print(f"Persons without antipsychotic purchases (SUB=2): {(fy['SUB'] == 2).sum():,}")

    # Supporting crosstabs
    print("\nSupporting crosstabs for new variables:")
    print(pd.crosstab(fy['SUB'], fy['N_PHRCHASE'] > 0, margins=True))

    # Step 4: Calculate estimates on expenditures and use
    print("\n" + "=" * 70)
    print("PERSON-LEVEL ESTIMATES ON EXPENDITURES AND USE FOR ANTIPSYCHOTIC DRUGS, 2015")
    print("=" * 70)

    # Filter to persons with antipsychotic purchases (SUB=1)
    fy_sub = fy[fy['SUB'] == 1].copy()

    # Create survey design for subpopulation
    design = MEPSSurveyDesign(fy_sub, year=2015)

    # Calculate estimates
    print("\n" + "-" * 70)
    print("National Totals (Persons with Antipsychotic Purchases)")
    print("-" * 70)

    # Total expenditures
    tot_results = design.total('TOT')
    print(f"\nTotal Expenditures for Antipsychotics:")
    print(f"  Sum:     ${tot_results['Total'].values[0]:,.0f}")
    print(f"  SE:      ${tot_results['SE'].values[0]:,.0f}")

    # Total number of purchases
    purch_results = design.total('N_PHRCHASE')
    print(f"\nTotal Number of Purchases:")
    print(f"  Sum:     {purch_results['Total'].values[0]:,.0f}")
    print(f"  SE:      {purch_results['SE'].values[0]:,.0f}")

    # Total number of persons
    fy_sub['PERSON'] = 1
    design_pers = MEPSSurveyDesign(fy_sub, year=2015)
    pers_results = design_pers.total('PERSON')
    print(f"\nTotal Number of Persons with Antipsychotic Purchases:")
    print(f"  Sum:     {pers_results['Total'].values[0]:,.0f}")
    print(f"  SE:      {pers_results['SE'].values[0]:,.0f}")

    # Per-person averages
    print("\n" + "-" * 70)
    print("Per-Person Averages (Among Persons with Antipsychotic Purchases)")
    print("-" * 70)

    # Mean total expense
    mean_tot = design.mean('TOT')
    print(f"\nMean Total Expense per Person:")
    print(f"  Mean:    ${mean_tot['Mean'].values[0]:,.2f}")
    print(f"  SE:      ${mean_tot['SE'].values[0]:.2f}")

    # Mean out-of-pocket expense
    mean_oop = design.mean('OOP')
    print(f"\nMean Out-of-Pocket Expense per Person:")
    print(f"  Mean:    ${mean_oop['Mean'].values[0]:,.2f}")
    print(f"  SE:      ${mean_oop['SE'].values[0]:.2f}")

    # Mean third-party payer expense
    mean_third = design.mean('THIRD_PAYER')
    print(f"\nMean Third-Party Payer Expense per Person:")
    print(f"  Mean:    ${mean_third['Mean'].values[0]:,.2f}")
    print(f"  SE:      ${mean_third['SE'].values[0]:.2f}")

    # Mean number of purchases
    mean_purch = design.mean('N_PHRCHASE')
    print(f"\nMean Number of Purchases per Person:")
    print(f"  Mean:    {mean_purch['Mean'].values[0]:.2f}")
    print(f"  SE:      {mean_purch['SE'].values[0]:.4f}")

    print("\n" + "=" * 70)
    print("Analysis complete.")
    print("=" * 70)


if __name__ == "__main__":
    main()
