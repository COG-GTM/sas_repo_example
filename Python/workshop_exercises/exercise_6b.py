"""
MEPS Workshop Exercise 6b: Logistic Regression for COVID Delayed Care, 2020

This program analyzes persons who delayed medical care because of COVID-19:
  - Percentage of people who delayed care
  - Logistic regression to identify demographic factors associated with
    delaying care due to COVID

Input file: C:/MEPS/h224.dta (2020 Full-Year Consolidated file)

This is the Python equivalent of the SAS program Exercise6b.sas
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.meps_loader import load_meps_data
from utils.survey_design import MEPSSurveyDesign


def main():
    # Read in data from 2020 FYC file
    fyc = load_meps_data("C:/MEPS/h224.dta")
    
    print("=" * 70)
    print("LOGISTIC REGRESSION FOR COVID DELAYED CARE, 2020")
    print("=" * 70)
    
    # COVID delayed care variable: CVDLAY42
    # 1 = Yes, delayed care due to COVID
    # 2 = No
    # -1 = Inapplicable
    # -7/-8/-9 = Missing
    
    # Select relevant variables
    vars_needed = ['DUPERSID', 'VARSTR', 'VARPSU', 'PERWT20F',
                   'CVDLAY42', 'AGELAST', 'SEX', 'RACETHX', 'INSCOV20', 'POVCAT20']
    fyc_sub = fyc[vars_needed].copy()
    
    # Create delayed care indicator
    fyc_sub['delayed_care'] = np.where(fyc_sub['CVDLAY42'] == 1, 1,
                              np.where(fyc_sub['CVDLAY42'] == 2, 0, np.nan))
    
    # Create subpopulation for valid responses (adults 18+)
    fyc_sub['subpop'] = (
        (fyc_sub['CVDLAY42'].isin([1, 2])) & 
        (fyc_sub['AGELAST'] >= 18)
    ).astype(int)
    
    # Create categorical variable labels
    sex_labels = {1: 'Male', 2: 'Female'}
    race_labels = {1: 'Hispanic', 2: 'NH White', 3: 'NH Black', 
                   4: 'NH Asian', 5: 'NH Other'}
    ins_labels = {1: 'Any Private', 2: 'Public Only', 3: 'Uninsured'}
    pov_labels = {1: 'Poor/Negative', 2: 'Near Poor', 3: 'Low Income',
                  4: 'Middle Income', 5: 'High Income'}
    
    fyc_sub['SEX_LABEL'] = fyc_sub['SEX'].map(sex_labels)
    fyc_sub['RACE_LABEL'] = fyc_sub['RACETHX'].map(race_labels)
    fyc_sub['INS_LABEL'] = fyc_sub['INSCOV20'].map(ins_labels)
    fyc_sub['POV_LABEL'] = fyc_sub['POVCAT20'].map(pov_labels)
    
    print(f"\nTotal observations: {len(fyc_sub):,}")
    print(f"Adults 18+ with valid response: {fyc_sub['subpop'].sum():,}")
    
    # QC: Check variable distributions
    print("\nDelayed care distribution (unweighted, adults 18+):")
    print(fyc_sub[fyc_sub['subpop'] == 1]['delayed_care'].value_counts())
    
    # Define survey design
    design = MEPSSurveyDesign(
        data=fyc_sub,
        strata='VARSTR',
        cluster='VARPSU',
        weights='PERWT20F'
    )
    
    # Subset to valid responses
    design_valid = design.subset(fyc_sub['subpop'] == 1)
    
    # Percentage of people who delayed care
    print("\n" + "-" * 70)
    print("PERCENTAGE OF ADULTS 18+ WHO DELAYED CARE DUE TO COVID")
    print("-" * 70)
    
    pct_delayed = design_valid.mean('delayed_care')
    print(f"Proportion: {pct_delayed['mean'].values[0]:.4f}")
    print(f"SE: {pct_delayed['se'].values[0]:.5f}")
    
    # Delayed care by demographic groups
    print("\n" + "-" * 70)
    print("DELAYED CARE PROPORTION BY DEMOGRAPHIC GROUPS")
    print("-" * 70)
    
    print("\nBy Sex:")
    by_sex = design_valid.mean('delayed_care', domain='SEX_LABEL')
    for _, row in by_sex.iterrows():
        print(f"  {row['domain_value']:15s}: {row['mean']:.4f} (SE: {row['se']:.5f})")
    
    print("\nBy Race/Ethnicity:")
    by_race = design_valid.mean('delayed_care', domain='RACE_LABEL')
    for _, row in by_race.iterrows():
        print(f"  {row['domain_value']:15s}: {row['mean']:.4f} (SE: {row['se']:.5f})")
    
    print("\nBy Insurance Status:")
    by_ins = design_valid.mean('delayed_care', domain='INS_LABEL')
    for _, row in by_ins.iterrows():
        print(f"  {row['domain_value']:15s}: {row['mean']:.4f} (SE: {row['se']:.5f})")
    
    print("\nBy Poverty Status:")
    by_pov = design_valid.mean('delayed_care', domain='POV_LABEL')
    for _, row in by_pov.iterrows():
        print(f"  {row['domain_value']:15s}: {row['mean']:.4f} (SE: {row['se']:.5f})")
    
    # Logistic regression
    print("\n" + "-" * 70)
    print("LOGISTIC REGRESSION: FACTORS ASSOCIATED WITH DELAYED CARE")
    print("-" * 70)
    
    # Prepare data for regression
    reg_data = fyc_sub[fyc_sub['subpop'] == 1].dropna(subset=['delayed_care'])
    
    design_reg = MEPSSurveyDesign(
        data=reg_data,
        strata='VARSTR',
        cluster='VARPSU',
        weights='PERWT20F'
    )
    
    # Fit logistic regression
    result = design_reg.glm(
        'delayed_care ~ AGELAST + C(SEX, Treatment(reference=1)) + '
        'C(RACETHX, Treatment(reference=1)) + C(INSCOV20, Treatment(reference=1)) + '
        'C(POVCAT20, Treatment(reference=5))',
        family='binomial'
    )
    
    print("\nLogistic Regression Results:")
    print(result.summary())


if __name__ == "__main__":
    main()
