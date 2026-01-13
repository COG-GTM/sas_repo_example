"""
Exercise 1b: National Health Care Expenses by Type of Service, 2015

This program generates the following estimates on national health care expenses
by type of service, 2015:
    (1) Percentage distribution of expenses by type of service
    (2) Percentage of persons with an expense, by type of service
    (3) Mean expense per person with an expense, by type of service

Defined service categories are:
    - Hospital Inpatient
    - Ambulatory Service: Office-based & Hospital Outpatient Visits
    - Prescribed Medicines
    - Dental Visits
    - Emergency Room
    - Home Health Care (Agency & Non-Agency) and Other

Note: Expenses include both facility and physician expenses.

Input file: C:/MEPS/h181.sas7bdat (2015 Full-Year Consolidated file)

This is a Python translation of the SAS program Exercise1b.sas
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
    print("EXERCISE 1b: NATIONAL HEALTH CARE EXPENSES BY TYPE OF SERVICE, 2015")
    print("=" * 70)

    # Load 2015 Full-Year Consolidated file (HC-181)
    filepath = "C:/MEPS/h181.sas7bdat"

    print(f"\nLoading data from: {filepath}")
    try:
        data = load_meps_data(filepath)
    except FileNotFoundError:
        print(f"Error: File not found at {filepath}")
        print("Please download the 2015 FYC file (h181) from the MEPS website:")
        print("https://meps.ahrq.gov/mepsweb/data_stats/download_data_files.jsp")
        return

    # Keep only needed variables
    keep_vars = [
        'TOTEXP15', 'IPDEXP15', 'IPFEXP15', 'OBVEXP15', 'RXEXP15',
        'OPDEXP15', 'OPFEXP15', 'DVTEXP15', 'ERDEXP15', 'ERFEXP15',
        'HHAEXP15', 'HHNEXP15', 'OTHEXP15', 'VISEXP15',
        'AGE15X', 'AGE42X', 'AGE31X', 'VARSTR', 'VARPSU', 'PERWT15F'
    ]
    data = data[keep_vars].copy()

    # Define expenditure variables by type of service
    data['TOTAL'] = data['TOTEXP15']
    data['HOSPITAL_INPATIENT'] = data['IPDEXP15'] + data['IPFEXP15']
    data['AMBULATORY'] = (data['OBVEXP15'] + data['OPDEXP15'] + data['OPFEXP15'] +
                          data['ERDEXP15'] + data['ERFEXP15'])
    data['PRESCRIBED_MEDICINES'] = data['RXEXP15']
    data['DENTAL'] = data['DVTEXP15']
    data['HOME_HEALTH_OTHER'] = (data['HHAEXP15'] + data['HHNEXP15'] +
                                  data['OTHEXP15'] + data['VISEXP15'])

    # QC check: sum of expenditures by type should equal total
    data['DIFF'] = (data['TOTAL'] - data['HOSPITAL_INPATIENT'] - data['AMBULATORY'] -
                    data['PRESCRIBED_MEDICINES'] - data['DENTAL'] - data['HOME_HEALTH_OTHER'])

    # Create flag (1/0) variables for persons with an expense, by type of service
    expense_vars = ['TOTAL', 'HOSPITAL_INPATIENT', 'AMBULATORY',
                    'PRESCRIBED_MEDICINES', 'DENTAL', 'HOME_HEALTH_OTHER']
    flag_vars = ['X_ANYSVCE', 'X_HOSPITAL_INPATIENT', 'X_AMBULATORY',
                 'X_PRESCRIBED_MEDICINES', 'X_DENTAL', 'X_HOME_HEALTH_OTHER']

    for exp_var, flag_var in zip(expense_vars, flag_vars):
        data[flag_var] = (data[exp_var] > 0).astype(int)

    # Create age variable
    data['AGE'] = np.where(
        data['AGE15X'] >= 0, data['AGE15X'],
        np.where(data['AGE42X'] >= 0, data['AGE42X'],
                 np.where(data['AGE31X'] >= 0, data['AGE31X'], np.nan))
    )

    # Create age category
    data['AGECAT'] = np.where(
        (data['AGE'] >= 0) & (data['AGE'] <= 64), 1,
        np.where(data['AGE'] > 64, 2, np.nan)
    )

    agecat_labels = {1: '0-64', 2: '65+', np.nan: 'All Ages'}

    # Supporting crosstabs
    print("\n" + "-" * 70)
    print("Supporting crosstabs for the flag variables")
    print("-" * 70)

    print("\nQC Check - DIFF should be 0 for all records:")
    print(data['DIFF'].describe())

    # Create survey design
    design = MEPSSurveyDesign(data, year=2015)

    # Percentage distribution of expenses by type of service
    print("\n" + "=" * 70)
    print("PERCENTAGE DISTRIBUTION OF EXPENSES BY TYPE OF SERVICE")
    print("=" * 70)

    service_types = [
        ('HOSPITAL_INPATIENT', 'Hospital Inpatient'),
        ('AMBULATORY', 'Ambulatory'),
        ('PRESCRIBED_MEDICINES', 'Prescribed Medicines'),
        ('DENTAL', 'Dental'),
        ('HOME_HEALTH_OTHER', 'Home Health & Other')
    ]

    total_exp = design.total('TOTAL')
    total_value = total_exp['Total'].values[0]

    print(f"\nTotal Health Care Expenses: ${total_value:,.0f}")
    print("\nDistribution by Type of Service:")
    print("-" * 50)

    for var_name, label in service_types:
        svc_total = design.total(var_name)
        svc_value = svc_total['Total'].values[0]
        pct = (svc_value / total_value) * 100 if total_value > 0 else 0
        print(f"  {label:25s}: ${svc_value:>15,.0f} ({pct:5.1f}%)")

    # Percentage of persons with an expense, by type of service
    print("\n" + "=" * 70)
    print("PERCENTAGE OF PERSONS WITH AN EXPENSE, BY TYPE OF SERVICE")
    print("=" * 70)

    flag_labels = [
        ('X_ANYSVCE', 'Any Service'),
        ('X_HOSPITAL_INPATIENT', 'Hospital Inpatient'),
        ('X_AMBULATORY', 'Ambulatory'),
        ('X_PRESCRIBED_MEDICINES', 'Prescribed Medicines'),
        ('X_DENTAL', 'Dental'),
        ('X_HOME_HEALTH_OTHER', 'Home Health & Other')
    ]

    print(f"\n{'Service Type':25s} {'N':>10s} {'Proportion':>12s} {'SE':>10s} {'Sum':>15s}")
    print("-" * 75)

    for flag_var, label in flag_labels:
        results = design.mean(flag_var)
        total_results = design.total(flag_var)
        print(f"  {label:23s} {results['N'].values[0]:>10,.0f} "
              f"{results['Mean'].values[0]:>12.4f} {results['SE'].values[0]:>10.5f} "
              f"{total_results['Total'].values[0]:>15,.0f}")

    # Mean expense per person with an expense, by type of service and age
    print("\n" + "=" * 70)
    print("MEAN EXPENSE PER PERSON WITH AN EXPENSE, BY TYPE OF SERVICE")
    print("=" * 70)

    # Total expenses
    print("\nMEAN TOTAL EXPENSE PER PERSON WITH AN EXPENSE:")
    data_with_expense = data[data['X_ANYSVCE'] == 1].copy()
    design_exp = MEPSSurveyDesign(data_with_expense, year=2015)

    print(f"\n{'Age Group':15s} {'N':>10s} {'Population':>15s} {'Mean($)':>12s} {'SE':>10s}")
    print("-" * 65)

    # Overall
    overall = design_exp.mean('TOTAL')
    print(f"  {'All Ages':13s} {overall['N'].values[0]:>10,.0f} "
          f"{overall['SumWgt'].values[0]:>15,.0f} "
          f"{overall['Mean'].values[0]:>12,.0f} {overall['SE'].values[0]:>10.3f}")

    # By age group
    for agecat_val in [1, 2]:
        subset = data_with_expense[data_with_expense['AGECAT'] == agecat_val].copy()
        if len(subset) > 0:
            design_subset = MEPSSurveyDesign(subset, year=2015)
            age_results = design_subset.mean('TOTAL')
            label = agecat_labels[agecat_val]
            print(f"  {label:13s} {age_results['N'].values[0]:>10,.0f} "
                  f"{age_results['SumWgt'].values[0]:>15,.0f} "
                  f"{age_results['Mean'].values[0]:>12,.0f} {age_results['SE'].values[0]:>10.3f}")

    # By service type
    service_expense_pairs = [
        ('HOSPITAL_INPATIENT', 'X_HOSPITAL_INPATIENT', 'Hospital Inpatient'),
        ('AMBULATORY', 'X_AMBULATORY', 'Ambulatory'),
        ('PRESCRIBED_MEDICINES', 'X_PRESCRIBED_MEDICINES', 'Prescribed Medicines'),
        ('DENTAL', 'X_DENTAL', 'Dental'),
        ('HOME_HEALTH_OTHER', 'X_HOME_HEALTH_OTHER', 'Home Health & Other')
    ]

    for exp_var, flag_var, label in service_expense_pairs:
        print(f"\nMEAN {label.upper()} EXPENSE PER PERSON WITH {label.upper()} EXPENSE:")
        print(f"{'Age Group':15s} {'N':>10s} {'Population':>15s} {'Mean($)':>12s} {'SE':>10s}")
        print("-" * 65)

        svc_data = data[data[flag_var] == 1].copy()
        if len(svc_data) > 0:
            design_svc = MEPSSurveyDesign(svc_data, year=2015)

            # Overall
            overall_svc = design_svc.mean(exp_var)
            print(f"  {'All Ages':13s} {overall_svc['N'].values[0]:>10,.0f} "
                  f"{overall_svc['SumWgt'].values[0]:>15,.0f} "
                  f"{overall_svc['Mean'].values[0]:>12,.0f} {overall_svc['SE'].values[0]:>10.3f}")

            # By age group
            for agecat_val in [1, 2]:
                subset = svc_data[svc_data['AGECAT'] == agecat_val].copy()
                if len(subset) > 0:
                    design_subset = MEPSSurveyDesign(subset, year=2015)
                    age_results = design_subset.mean(exp_var)
                    age_label = agecat_labels[agecat_val]
                    print(f"  {age_label:13s} {age_results['N'].values[0]:>10,.0f} "
                          f"{age_results['SumWgt'].values[0]:>15,.0f} "
                          f"{age_results['Mean'].values[0]:>12,.0f} {age_results['SE'].values[0]:>10.3f}")

    print("\n" + "=" * 70)
    print("Analysis complete.")
    print("=" * 70)


if __name__ == "__main__":
    main()
