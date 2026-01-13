"""
Example code to replicate estimates from the MEPS-HC Data Tools summary tables

Use, expenditures, and population: Expenditures, 2016

Expenditures for office-based and outpatient visits, by source of payment

Input file: 2016 full-year consolidated (h192)

This is the Python equivalent of R/summary_tables_examples/use_expenditures_2016.R
"""

import sys
sys.path.insert(0, "../..")

import pandas as pd
import numpy as np

from meps import read_meps, MEPSSurveyDesign, svymean, svytotal
from meps.utils import format_currency


def main():
    print("Loading 2016 FYC data...")
    fyc = read_meps(year=2016, type="FYC")

    print("\nDefining survey design...")
    design = MEPSSurveyDesign(
        data=fyc,
        id_var="VARPSU",
        strata_var="VARSTR",
        weight_var="PERWT16F",
    )

    print("\n" + "=" * 70)
    print("EXPENDITURES FOR OFFICE-BASED VISITS BY SOURCE OF PAYMENT")
    print("=" * 70)

    ob_vars = ["OBVEXP16", "OBVSLF16", "OBVMCR16", "OBVMCD16", "OBVPRV16", "OBVOTH16"]
    ob_labels = {
        "OBVEXP16": "Total",
        "OBVSLF16": "Out-of-pocket/Self",
        "OBVMCR16": "Medicare",
        "OBVMCD16": "Medicaid",
        "OBVPRV16": "Private Insurance",
        "OBVOTH16": "Other",
    }

    print("\n--- Total Expenditures (svytotal) ---")
    for var in ob_vars:
        if var in fyc.columns:
            result = svytotal(design, var)
            label = ob_labels.get(var, var)
            total = result["total"].values[0]
            se = result["SE"].values[0]
            print(f"  {label}: {format_currency(total)} (SE: {format_currency(se)})")

    print("\n--- Mean Expenditure per Person (svymean) ---")
    for var in ob_vars:
        if var in fyc.columns:
            result = svymean(design, var)
            label = ob_labels.get(var, var)
            mean = result["mean"].values[0]
            se = result["SE"].values[0]
            print(f"  {label}: {format_currency(mean, 2)} (SE: {format_currency(se, 2)})")

    print("\n" + "=" * 70)
    print("EXPENDITURES FOR OUTPATIENT VISITS BY SOURCE OF PAYMENT")
    print("=" * 70)

    op_vars = ["OPTEXP16", "OPTSLF16", "OPTMCR16", "OPTMCD16", "OPTPRV16", "OPTOTH16"]
    op_labels = {
        "OPTEXP16": "Total",
        "OPTSLF16": "Out-of-pocket/Self",
        "OPTMCR16": "Medicare",
        "OPTMCD16": "Medicaid",
        "OPTPRV16": "Private Insurance",
        "OPTOTH16": "Other",
    }

    print("\n--- Total Expenditures (svytotal) ---")
    for var in op_vars:
        if var in fyc.columns:
            result = svytotal(design, var)
            label = op_labels.get(var, var)
            total = result["total"].values[0]
            se = result["SE"].values[0]
            print(f"  {label}: {format_currency(total)} (SE: {format_currency(se)})")

    print("\n--- Mean Expenditure per Person (svymean) ---")
    for var in op_vars:
        if var in fyc.columns:
            result = svymean(design, var)
            label = op_labels.get(var, var)
            mean = result["mean"].values[0]
            se = result["SE"].values[0]
            print(f"  {label}: {format_currency(mean, 2)} (SE: {format_currency(se, 2)})")


if __name__ == "__main__":
    main()
