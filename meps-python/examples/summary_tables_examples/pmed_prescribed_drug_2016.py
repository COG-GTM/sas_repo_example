"""
Example code to replicate estimates from the MEPS-HC Data Tools summary tables

Prescribed drugs: Purchases and expenditures by generic drug name, 2016

Input file: 2016 Prescribed Medicines file (h188a)

This is the Python equivalent of R/summary_tables_examples/pmed_prescribed_drug_2016.R
"""

import sys
sys.path.insert(0, "../..")

import pandas as pd
import numpy as np

from meps import read_meps, MEPSSurveyDesign, svymean, svytotal, svyby
from meps.utils import format_currency


def main():
    print("Loading 2016 Prescribed Medicines data...")
    pmed = read_meps(year=2016, type="PMED")

    print(f"Total records: {len(pmed)}")

    print("\nViewing top drug names by frequency...")
    top_drugs = pmed.groupby("RXDRGNAM").size().sort_values(ascending=False).head(20)
    print(top_drugs)

    print("\nDefining survey design...")
    design = MEPSSurveyDesign(
        data=pmed,
        id_var="VARPSU",
        strata_var="VARSTR",
        weight_var="PERWT16F",
    )

    print("\n" + "=" * 70)
    print("TOTAL EXPENDITURES AND PURCHASES FOR TOP 10 DRUGS")
    print("=" * 70)

    top_10_drugs = top_drugs.head(10).index.tolist()

    results = []
    for drug in top_10_drugs:
        drug_condition = pmed["RXDRGNAM"] == drug
        drug_design = design.subset(drug_condition)

        pmed["_one"] = 1
        total_result = svytotal(drug_design, "RXXP16X")
        count_result = svytotal(drug_design, "_one")

        results.append({
            "Drug": drug,
            "Total_Expenditure": total_result["total"].values[0],
            "SE_Expenditure": total_result["SE"].values[0],
            "Total_Purchases": count_result["total"].values[0],
            "SE_Purchases": count_result["SE"].values[0],
        })

    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values("Total_Expenditure", ascending=False)

    print("\nResults:")
    for _, row in results_df.iterrows():
        print(f"\n{row['Drug']}:")
        print(f"  Total Expenditure: {format_currency(row['Total_Expenditure'])} (SE: {format_currency(row['SE_Expenditure'])})")
        print(f"  Total Purchases: {row['Total_Purchases']:,.0f} (SE: {row['SE_Purchases']:,.0f})")


if __name__ == "__main__":
    main()
