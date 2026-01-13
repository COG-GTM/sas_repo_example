"""
MEPS Workshop Exercise 2a: Trends in Antipsychotics Purchases and Expenses, 2015

This program analyzes prescribed medicine purchases for antipsychotics,
including number of purchases and total expenditures.

Input file: C:/MEPS/h178a.ssp (2015 Prescribed Medicines file)

This is the Python equivalent of the SAS program Exercise2a.sas
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.meps_loader import load_meps_data
from utils.survey_design import MEPSSurveyDesign


def main():
    # Read in data from 2015 Prescribed Medicines file (HC-178A)
    h178a = load_meps_data("C:/MEPS/h178a.ssp")
    
    # Therapeutic class code for antipsychotics
    # TC1S1_1 = 242 (Antipsychotics)
    ANTIPSYCH_TC = 242
    
    # Filter to antipsychotic medications
    antipsych = h178a[h178a['TC1S1_1'] == ANTIPSYCH_TC].copy()
    
    print("=" * 60)
    print("ANTIPSYCHOTICS PURCHASES AND EXPENSES, 2015")
    print("=" * 60)
    
    print(f"\nNumber of antipsychotic prescription records: {len(antipsych):,}")
    
    # Create person-level summary
    # Aggregate to person level
    person_summary = antipsych.groupby(
        ['DUPERSID', 'VARSTR', 'VARPSU', 'PERWT15F']
    ).agg({
        'RXXP15X': ['sum', 'count']
    }).reset_index()
    
    person_summary.columns = ['DUPERSID', 'VARSTR', 'VARPSU', 'PERWT15F', 
                              'total_exp', 'n_purchases']
    person_summary['person'] = 1
    
    # Define survey design
    design = MEPSSurveyDesign(
        data=person_summary,
        strata='VARSTR',
        cluster='VARPSU',
        weights='PERWT15F'
    )
    
    # Number of people with antipsychotic purchases
    print("\n" + "-" * 60)
    print("NUMBER OF PEOPLE WITH ANTIPSYCHOTIC PURCHASES")
    n_people = design.total('person')
    print(f"Total: {n_people['total'].values[0]:,.0f}")
    print(f"SE: {n_people['se'].values[0]:,.0f}")
    
    # Total number of purchases
    print("\n" + "-" * 60)
    print("TOTAL NUMBER OF ANTIPSYCHOTIC PURCHASES")
    total_purchases = design.total('n_purchases')
    print(f"Total: {total_purchases['total'].values[0]:,.0f}")
    print(f"SE: {total_purchases['se'].values[0]:,.0f}")
    
    # Total expenditures
    print("\n" + "-" * 60)
    print("TOTAL EXPENDITURES FOR ANTIPSYCHOTICS")
    total_exp = design.total('total_exp')
    print(f"Total: ${total_exp['total'].values[0]:,.0f}")
    print(f"SE: ${total_exp['se'].values[0]:,.0f}")
    
    # Mean expenditure per person
    print("\n" + "-" * 60)
    print("MEAN EXPENDITURE PER PERSON WITH ANTIPSYCHOTIC PURCHASES")
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
