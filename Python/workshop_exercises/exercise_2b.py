"""
MEPS Workshop Exercise 2b: Narcotic Analgesics Purchases and Expenses, 2016

This program analyzes prescribed medicine purchases for narcotic analgesics
or narcotic analgesic combinations, including number of purchases and 
total expenditures.

Input file: C:/MEPS/h188a.ssp (2016 Prescribed Medicines file)

This is the Python equivalent of the SAS program Exercise2b.sas
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.meps_loader import load_meps_data
from utils.survey_design import MEPSSurveyDesign


def main():
    # Read in data from 2016 Prescribed Medicines file (HC-188A)
    h188a = load_meps_data("C:/MEPS/h188a.ssp")
    
    # Therapeutic class codes for narcotic analgesics
    # TC1S1_1 = 60 (Narcotic analgesics)
    # TC1S1_1 = 191 (Narcotic analgesic combos)
    NARCOTIC_TC = [60, 191]
    
    # Filter to narcotic analgesic medications
    narcotics = h188a[h188a['TC1S1_1'].isin(NARCOTIC_TC)].copy()
    
    print("=" * 60)
    print("NARCOTIC ANALGESICS PURCHASES AND EXPENSES, 2016")
    print("=" * 60)
    
    print(f"\nNumber of narcotic prescription records: {len(narcotics):,}")
    
    # Create person-level summary
    person_summary = narcotics.groupby(
        ['DUPERSID', 'VARSTR', 'VARPSU', 'PERWT16F']
    ).agg({
        'RXXP16X': ['sum', 'count']
    }).reset_index()
    
    person_summary.columns = ['DUPERSID', 'VARSTR', 'VARPSU', 'PERWT16F',
                              'total_exp', 'n_purchases']
    person_summary['person'] = 1
    
    # Define survey design
    design = MEPSSurveyDesign(
        data=person_summary,
        strata='VARSTR',
        cluster='VARPSU',
        weights='PERWT16F'
    )
    
    # Number of people with narcotic purchases
    print("\n" + "-" * 60)
    print("NUMBER OF PEOPLE WITH NARCOTIC ANALGESIC PURCHASES")
    n_people = design.total('person')
    print(f"Total: {n_people['total'].values[0]:,.0f}")
    print(f"SE: {n_people['se'].values[0]:,.0f}")
    
    # Total number of purchases
    print("\n" + "-" * 60)
    print("TOTAL NUMBER OF NARCOTIC ANALGESIC PURCHASES")
    total_purchases = design.total('n_purchases')
    print(f"Total: {total_purchases['total'].values[0]:,.0f}")
    print(f"SE: {total_purchases['se'].values[0]:,.0f}")
    
    # Total expenditures
    print("\n" + "-" * 60)
    print("TOTAL EXPENDITURES FOR NARCOTIC ANALGESICS")
    total_exp = design.total('total_exp')
    print(f"Total: ${total_exp['total'].values[0]:,.0f}")
    print(f"SE: ${total_exp['se'].values[0]:,.0f}")
    
    # Mean expenditure per person
    print("\n" + "-" * 60)
    print("MEAN EXPENDITURE PER PERSON WITH NARCOTIC PURCHASES")
    mean_exp = design.mean('total_exp')
    print(f"Mean: ${mean_exp['mean'].values[0]:,.2f}")
    print(f"SE: ${mean_exp['se'].values[0]:,.2f}")
    
    # Mean number of purchases per person
    print("\n" + "-" * 60)
    print("MEAN NUMBER OF PURCHASES PER PERSON")
    mean_purchases = design.mean('n_purchases')
    print(f"Mean: {mean_purchases['mean'].values[0]:.2f}")
    print(f"SE: {mean_purchases['se'].values[0]:.2f}")


if __name__ == "__main__":
    main()
