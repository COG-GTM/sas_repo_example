"""
MEPS Summary Tables Example: Use, Expenditures, and Population, 2018

This code replicates estimates from the MEPS-HC Data Tools summary tables for
Use, Expenditures, and Population, 2018

Estimates:
  - Total health care expenditures
  - Mean expenditure per person
  - By type of service

Input file: C:/MEPS/h209.dta (2018 Full-Year Consolidated file)

This is the Python equivalent of use_expenditures_2018.sas
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.meps_loader import load_meps_data
from utils.survey_design import MEPSSurveyDesign


def main():
    # Load FYC file
    fyc = load_meps_data("C:/MEPS/h209.dta")
    
    print("=" * 70)
    print("USE, EXPENDITURES, AND POPULATION, 2018")
    print("=" * 70)
    
    # Expenditure variables by type of service
    exp_vars = {
        'TOTEXP18': 'Total',
        'OBVEXP18': 'Office-based',
        'OPTEXP18': 'Outpatient',
        'ERTEXP18': 'Emergency room',
        'IPTEXP18': 'Inpatient',
        'RXEXP18': 'Prescribed medicines',
        'DVTEXP18': 'Dental',
        'HHAEXP18': 'Home health',
        'OTHEXP18': 'Other'
    }
    
    # Create person indicator
    fyc['person'] = 1
    
    # Create indicator for any expense
    fyc['any_expense'] = (fyc['TOTEXP18'] > 0).astype(int)
    
    print(f"\nTotal observations: {len(fyc):,}")
    
    # Define survey design
    design = MEPSSurveyDesign(
        data=fyc,
        strata='VARSTR',
        cluster='VARPSU',
        weights='PERWT18F'
    )
    
    # Total population
    print("\n" + "-" * 70)
    print("TOTAL POPULATION")
    print("-" * 70)
    
    n_pop = design.total('person')
    print(f"Total: {n_pop['total'].values[0]:,.0f} (SE: {n_pop['se'].values[0]:,.0f})")
    
    # Percent with any expense
    pct_expense = design.mean('any_expense')
    print(f"Percent with any expense: {pct_expense['mean'].values[0]:.4f}")
    
    # Expenditures by type of service
    print("\n" + "-" * 70)
    print("EXPENDITURES BY TYPE OF SERVICE")
    print("-" * 70)
    
    for var, label in exp_vars.items():
        total_exp = design.total(var)
        mean_exp = design.mean(var)
        
        print(f"\n{label}:")
        print(f"  Total: ${total_exp['total'].values[0]:,.0f} (SE: ${total_exp['se'].values[0]:,.0f})")
        print(f"  Mean per person: ${mean_exp['mean'].values[0]:,.2f} (SE: ${mean_exp['se'].values[0]:,.2f})")
    
    # Mean expenditure among those with expense
    print("\n" + "-" * 70)
    print("MEAN EXPENDITURE AMONG THOSE WITH ANY EXPENSE")
    print("-" * 70)
    
    design_expense = design.subset(fyc['any_expense'] == 1)
    mean_among_expense = design_expense.mean('TOTEXP18')
    print(f"Mean: ${mean_among_expense['mean'].values[0]:,.2f}")
    print(f"SE: ${mean_among_expense['se'].values[0]:,.2f}")


if __name__ == "__main__":
    main()
