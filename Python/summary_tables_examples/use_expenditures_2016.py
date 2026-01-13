"""
Healthcare Utilization Summary Table: Expenditures by Service Type, 2016

This program generates estimates on healthcare expenditures:
    - Total expenditures by type of service
    - Mean expenditure per person
    - Percentage with any expense

Input file:
    - 2016 Full-year consolidated file (h192)

This is a Python translation of the R program use_expenditures_2016.R
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
    print("MEPS SUMMARY TABLE: EXPENDITURES BY SERVICE TYPE, 2016")
    print("=" * 70)

    # Load 2016 Full-Year Consolidated file
    fyc_path = "C:/MEPS/h192.sas7bdat"

    print(f"\nLoading data from: {fyc_path}")

    try:
        fyc = load_meps_data(fyc_path)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please download the 2016 FYC file (h192) from the MEPS website:")
        print("https://meps.ahrq.gov/mepsweb/data_stats/download_data_files.jsp")
        return

    # Define expenditure variables by service type
    exp_vars = {
        'Total': 'TOTEXP16',
        'Inpatient': 'IPTEXP16',
        'Outpatient': 'OPTEXP16',
        'Office-Based': 'OBVEXP16',
        'Emergency Room': 'ERTEXP16',
        'Dental': 'DVTEXP16',
        'Home Health': 'HHAEXP16',
        'Rx Medicines': 'RXEXP16',
        'Other': 'OTHEXP16',
        'Vision': 'VISEXP16',
    }

    # Keep needed variables
    kept_vars = ['VARSTR', 'VARPSU', 'PERWT16F'] + list(exp_vars.values())
    available_vars = [v for v in kept_vars if v in fyc.columns]
    data = fyc[available_vars].copy()

    print(f"Records loaded: {len(data):,}")

    # Create flag variables for any expense
    for label, var in exp_vars.items():
        if var in data.columns:
            flag_name = f'X_{label.upper().replace(" ", "_").replace("-", "_")}'
            data[flag_name] = (data[var] > 0).astype(int)

    # Create survey design
    design = MEPSSurveyDesign(data, year=2016)

    # Calculate estimates by service type
    print("\n" + "=" * 70)
    print("EXPENDITURES BY TYPE OF SERVICE")
    print("=" * 70)

    print(f"\n{'Service Type':20s} {'Total ($B)':>12s} {'Mean ($)':>12s} {'% w/Expense':>12s} {'SE (%)':>10s}")
    print("-" * 70)

    results_list = []
    for label, var in exp_vars.items():
        if var in data.columns:
            # Total expenditures
            total_results = design.total(var)
            total_billions = total_results['Total'].values[0] / 1e9
            
            # Mean expenditure
            mean_results = design.mean(var)
            mean_exp = mean_results['Mean'].values[0]
            
            # Percentage with expense
            flag_name = f'X_{label.upper().replace(" ", "_").replace("-", "_")}'
            if flag_name in data.columns:
                pct_results = design.mean(flag_name)
                pct = pct_results['Mean'].values[0] * 100
                se = pct_results['SE'].values[0] * 100
            else:
                pct = 0
                se = 0
            
            results_list.append({
                'Service': label,
                'Total_B': total_billions,
                'Mean': mean_exp,
                'Pct': pct,
                'SE': se
            })
            
            print(f"{label:20s} ${total_billions:>11.1f} ${mean_exp:>11,.0f} {pct:>11.1f}% {se:>9.2f}%")

    # Expenditures by source of payment
    print("\n" + "=" * 70)
    print("TOTAL EXPENDITURES BY SOURCE OF PAYMENT")
    print("=" * 70)

    sop_vars = {
        'Out-of-Pocket': 'TOTSLF16',
        'Medicare': 'TOTMCR16',
        'Medicaid': 'TOTMCD16',
        'Private Insurance': 'TOTPRV16',
        'Other': 'TOTOTZ16',
    }

    print(f"\n{'Source of Payment':20s} {'Total ($B)':>12s} {'% of Total':>12s}")
    print("-" * 50)

    total_exp = data['TOTEXP16'].sum() if 'TOTEXP16' in data.columns else 0
    
    for label, var in sop_vars.items():
        if var in data.columns:
            total_results = design.total(var)
            total_billions = total_results['Total'].values[0] / 1e9
            
            # Calculate percentage of total
            if 'TOTEXP16' in data.columns:
                total_all = design.total('TOTEXP16')
                pct = total_results['Total'].values[0] / total_all['Total'].values[0] * 100
            else:
                pct = 0
            
            print(f"{label:20s} ${total_billions:>11.1f} {pct:>11.1f}%")

    # Summary statistics
    print("\n" + "-" * 70)
    print("Summary Statistics")
    print("-" * 70)

    if 'TOTEXP16' in data.columns:
        total_results = design.total('TOTEXP16')
        mean_results = design.mean('TOTEXP16')
        
        print(f"\nTotal healthcare expenditures: ${total_results['Total'].values[0]/1e12:.2f} trillion")
        print(f"Mean expenditure per person: ${mean_results['Mean'].values[0]:,.0f}")
        print(f"SE of mean: ${mean_results['SE'].values[0]:,.0f}")

    print("\n" + "=" * 70)
    print("Analysis complete.")
    print("=" * 70)


if __name__ == "__main__":
    main()
