"""
This program illustrates how to pool MEPS data files from different years. It
highlights one example of a discontinuity that may be encountered when
working with data from before and after the 2018 CAPI re-design.

It also demonstrates use of the Pooled Variance file (h36u19) to pool data
years before and after 2019.

The program pools 2017, 2018, and 2019 data and calculates:
 - Percentage of people with Joint Pain / Arthritis (JTPAIN**, ARTHDX)
 - Average expenditures per person, by Joint Pain status (TOTEXP, TOTSLF)

Notes:
 - Variables with year-specific names must be renamed before combining files
   (e.g. 'TOTEXP18' and 'TOTEXP19' renamed to 'totexp')
 - When pooling data years before and after 2002 or 2019, the Pooled Variance
   file (h36u19) must be used for correct variance estimation

Input files:
 - 2019 Full-year file (h216)
 - 2018 Full-year file (h209)
 - 2017 Full-year file (h201)
 - Pooled Variance Linkage file (h36u19)

This is the Python equivalent of R/workshop_exercises/exercise_3d.R
"""

import sys
sys.path.insert(0, "../..")

import pandas as pd
import numpy as np

from meps import read_meps, MEPSSurveyDesign, svymean, svyby
from meps.utils import standardize_variable_names, pool_weights, combine_years


def create_jtpain_variable(df, year):
    """Create any_jtpain variable accounting for year-specific variable names."""
    if year >= 2018:
        jtpain_var = "JTPAIN31_M18"
    else:
        jtpain_var = "JTPAIN31"

    conditions = [
        (df[jtpain_var] == 1) | (df["ARTHDX"] == 1),
        (df[jtpain_var] < 0) & (df["ARTHDX"] < 0),
    ]
    choices = ["1 Yes", "Missing"]
    df["any_jtpain"] = np.select(conditions, choices, default="2 No")
    return df


def main():
    print("Loading data files...")
    print("  - 2019 FYC")
    fyc19 = read_meps(year=2019, type="FYC")
    print("  - 2018 FYC")
    fyc18 = read_meps(year=2018, type="FYC")
    print("  - 2017 FYC")
    fyc17 = read_meps(year=2017, type="FYC")
    print("  - Pooled Linkage file")
    linkage = read_meps(type="Pooled linkage")

    print("\nViewing JTPAIN and ARTHDX values for people with arthritis...")
    print("\n2017 (JTPAIN31):")
    print(fyc17[fyc17["ARTHDX"] == 1].groupby(["ARTHDX", "JTPAIN31"]).size().head(10))
    print("\n2018 (JTPAIN31_M18):")
    print(fyc18[fyc18["ARTHDX"] == 1].groupby(["ARTHDX", "JTPAIN31_M18"]).size().head(10))
    print("\n2019 (JTPAIN31_M18):")
    print(fyc19[fyc19["ARTHDX"] == 1].groupby(["ARTHDX", "JTPAIN31_M18"]).size().head(10))

    print("\nCreating any_jtpain variable...")
    fyc19 = create_jtpain_variable(fyc19, 2019)
    fyc18 = create_jtpain_variable(fyc18, 2018)
    fyc17 = create_jtpain_variable(fyc17, 2017)

    print("\nQC: any_jtpain for adults (AGELAST >= 18)")
    print("\n2019:")
    print(fyc19[fyc19["AGELAST"] >= 18].groupby("any_jtpain").size())
    print("\n2018:")
    print(fyc18[fyc18["AGELAST"] >= 18].groupby("any_jtpain").size())
    print("\n2017:")
    print(fyc17[fyc17["AGELAST"] >= 18].groupby("any_jtpain").size())

    print("\nRenaming year-specific variables...")

    fyc19p = fyc19.rename(columns={
        "PERWT19F": "perwt",
        "TOTSLF19": "totslf",
        "TOTEXP19": "totexp",
    })[["DUPERSID", "PANEL", "VARSTR", "VARPSU", "AGELAST", "perwt", "totslf", "totexp", "ARTHDX", "any_jtpain"]]

    fyc18p = fyc18.rename(columns={
        "PERWT18F": "perwt",
        "TOTSLF18": "totslf",
        "TOTEXP18": "totexp",
    })[["DUPERSID", "PANEL", "VARSTR", "VARPSU", "AGELAST", "perwt", "totslf", "totexp", "ARTHDX", "any_jtpain"]]

    fyc17p = fyc17.rename(columns={
        "PERWT17F": "perwt",
        "TOTSLF17": "totslf",
        "TOTEXP17": "totexp",
    })[["DUPERSID", "PANEL", "VARSTR", "VARPSU", "AGELAST", "perwt", "totslf", "totexp", "ARTHDX", "any_jtpain"]]

    print("\nStacking data and creating pooled weight...")
    pool = pd.concat([fyc19p, fyc18p, fyc17p], ignore_index=True)
    pool["poolwt"] = pool["perwt"] / 3
    pool["subpop"] = (pool["AGELAST"] >= 18) & (pool["any_jtpain"] != "Missing")

    print(f"  Total pooled records: {len(pool)}")

    print("\nMerging with Pooled Linkage Variance file...")
    linkage_sub = linkage[["DUPERSID", "PANEL", "STRA9619", "PSU9619"]].copy()
    pool_linked = pd.merge(pool, linkage_sub, on=["DUPERSID", "PANEL"], how="left")

    print("\nQC: Records by PANEL")
    print(pool_linked.groupby("PANEL").size())

    print("\nDefining survey design with PSU9619 and STRA9619...")
    pool_design = MEPSSurveyDesign(
        data=pool_linked,
        id_var="PSU9619",
        strata_var="STRA9619",
        weight_var="poolwt",
    )

    sub_design = pool_design.subset(pool_linked["subpop"])

    print("\n" + "=" * 70)
    print("SURVEY ESTIMATES")
    print("=" * 70)

    print("\nPercent with any joint pain (any_jtpain):")
    result = svymean(sub_design, "any_jtpain")
    print(result.to_string(index=False))

    print("\nAverage expenditures per person, by Joint Pain status:")
    result = svyby(sub_design, ["totslf", "totexp"], "any_jtpain", svymean)
    print(result.to_string(index=False))


if __name__ == "__main__":
    main()
