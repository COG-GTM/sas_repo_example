"""
Healthcare Utilization Summary Table: Events by Service Type, 2016

This program generates estimates on healthcare utilization:
    - Total number of events by type of service
    - Mean events per person
    - Percentage with any event

Input file:
    - 2016 Full-year consolidated file (h192)

This is a Python translation of the Stata program use_events_2016.do
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
    print("MEPS SUMMARY TABLE: EVENTS BY SERVICE TYPE, 2016")
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

    # Define utilization variables by service type
    use_vars = {
        'Office-Based': 'OBTOTV16',
        'Outpatient': 'OPTOTV16',
        'Emergency Room': 'ERTOT16',
        'Inpatient Stays': 'IPDIS16',
        'Inpatient Nights': 'IPNGTD16',
        'Dental': 'DVTOT16',
        'Home Health': 'HHTOTD16',
        'Rx Fills': 'RXTOT16',
    }

    # Keep needed variables
    kept_vars = ['VARSTR', 'VARPSU', 'PERWT16F'] + list(use_vars.values())
    available_vars = [v for v in kept_vars if v in fyc.columns]
    data = fyc[available_vars].copy()

    print(f"Records loaded: {len(data):,}")

    # Create flag variables for any utilization
    for label, var in use_vars.items():
        if var in data.columns:
            flag_name = f'X_{label.upper().replace(" ", "_").replace("-", "_")}'
            data[flag_name] = (data[var] > 0).astype(int)

    # Create survey design
    design = MEPSSurveyDesign(data, year=2016)

    # Calculate estimates by service type
    print("\n" + "=" * 70)
    print("UTILIZATION BY TYPE OF SERVICE")
    print("=" * 70)

    print(f"\n{'Service Type':20s} {'Total (M)':>12s} {'Mean':>10s} {'% w/Any':>12s} {'SE (%)':>10s}")
    print("-" * 70)

    for label, var in use_vars.items():
        if var in data.columns:
            # Total events
            total_results = design.total(var)
            total_millions = total_results['Total'].values[0] / 1e6
            
            # Mean events per person
            mean_results = design.mean(var)
            mean_events = mean_results['Mean'].values[0]
            
            # Percentage with any event
            flag_name = f'X_{label.upper().replace(" ", "_").replace("-", "_")}'
            if flag_name in data.columns:
                pct_results = design.mean(flag_name)
                pct = pct_results['Mean'].values[0] * 100
                se = pct_results['SE'].values[0] * 100
            else:
                pct = 0
                se = 0
            
            print(f"{label:20s} {total_millions:>11.1f} {mean_events:>9.2f} {pct:>11.1f}% {se:>9.2f}%")

    # Mean events among users
    print("\n" + "=" * 70)
    print("MEAN EVENTS AMONG USERS")
    print("=" * 70)

    print(f"\n{'Service Type':20s} {'N Users':>12s} {'Mean Events':>12s} {'SE':>10s}")
    print("-" * 60)

    for label, var in use_vars.items():
        if var in data.columns:
            flag_name = f'X_{label.upper().replace(" ", "_").replace("-", "_")}'
            if flag_name in data.columns:
                # Filter to users only
                users = data[data[flag_name] == 1].copy()
                if len(users) > 0:
                    design_users = MEPSSurveyDesign(users, year=2016)
                    mean_results = design_users.mean(var)
                    
                    print(f"{label:20s} {len(users):>12,} {mean_results['Mean'].values[0]:>11.2f} {mean_results['SE'].values[0]:>9.4f}")

    # Summary statistics
    print("\n" + "-" * 70)
    print("Summary Statistics")
    print("-" * 70)

    # Total population
    pop_total = data['PERWT16F'].sum()
    print(f"\nTotal population: {pop_total/1e6:,.1f} million")

    # Office visits
    if 'OBTOTV16' in data.columns:
        total_ob = design.total('OBTOTV16')
        print(f"Total office-based visits: {total_ob['Total'].values[0]/1e6:,.1f} million")

    # Rx fills
    if 'RXTOT16' in data.columns:
        total_rx = design.total('RXTOT16')
        print(f"Total Rx fills: {total_rx['Total'].values[0]/1e9:,.2f} billion")

    print("\n" + "=" * 70)
    print("Analysis complete.")
    print("=" * 70)


if __name__ == "__main__":
    main()
