"""
MEPS Workshop Exercise 6a: Logistic Regression for Flu Shot, 2018

This program includes a regression example for persons receiving a flu shot
in the last 12 months for the civilian noninstitutionalized population:
  - Percentage of people with a flu shot
  - Logistic regression to identify demographic factors associated with
    receiving a flu shot

Input file: C:/MEPS/h209.dta (2018 Full-Year Consolidated file)

This is the Python equivalent of the SAS program Exercise6.sas
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.meps_loader import load_meps_data
from utils.survey_design import MEPSSurveyDesign


def main():
    # Read in data from 2018 FYC file
    fyc = load_meps_data("C:/MEPS/h209.dta")
    
    print("=" * 70)
    print("LOGISTIC REGRESSION FOR FLU SHOT, 2018")
    print("=" * 70)
    
    # Select relevant variables
    vars_needed = ['DUPERSID', 'VARSTR', 'VARPSU', 'SAQWT18F',
                   'ADFLST42', 'AGELAST', 'SEX', 'RACETHX', 'INSCOV18']
    fyc_sub = fyc[vars_needed].copy()
    
    # Create flu shot indicator
    # ADFLST42: 1 = Yes, 2 = No, -1 = Inapplicable, -7/-8/-9 = Missing
    fyc_sub['flu_shot'] = np.where(fyc_sub['ADFLST42'] == 1, 1,
                          np.where(fyc_sub['ADFLST42'] == 2, 0, np.nan))
    
    # Create subpopulation for valid responses (adults 18+)
    fyc_sub['subpop'] = (
        (fyc_sub['ADFLST42'].isin([1, 2])) & 
        (fyc_sub['AGELAST'] >= 18)
    ).astype(int)
    
    # Create categorical variable labels
    sex_labels = {1: 'Male', 2: 'Female'}
    race_labels = {1: 'Hispanic', 2: 'NH White', 3: 'NH Black', 
                   4: 'NH Asian', 5: 'NH Other'}
    ins_labels = {1: 'Any Private', 2: 'Public Only', 3: 'Uninsured'}
    
    fyc_sub['SEX_LABEL'] = fyc_sub['SEX'].map(sex_labels)
    fyc_sub['RACE_LABEL'] = fyc_sub['RACETHX'].map(race_labels)
    fyc_sub['INS_LABEL'] = fyc_sub['INSCOV18'].map(ins_labels)
    
    print(f"\nTotal observations: {len(fyc_sub):,}")
    print(f"Adults 18+ with valid flu shot response: {fyc_sub['subpop'].sum():,}")
    
    # QC: Check variable distributions
    print("\nFlu shot distribution (unweighted, adults 18+):")
    print(fyc_sub[fyc_sub['subpop'] == 1]['flu_shot'].value_counts())
    
    # Define survey design using SAQ weight
    # Note: SAQWT18F is used for Self-Administered Questionnaire items like flu shot
    design = MEPSSurveyDesign(
        data=fyc_sub,
        strata='VARSTR',
        cluster='VARPSU',
        weights='SAQWT18F'
    )
    
    # Subset to valid responses
    design_valid = design.subset(fyc_sub['subpop'] == 1)
    
    # Percentage of people with a flu shot
    print("\n" + "-" * 70)
    print("PERCENTAGE OF ADULTS 18+ WITH A FLU SHOT")
    print("-" * 70)
    
    pct_flu = design_valid.mean('flu_shot')
    print(f"Proportion: {pct_flu['mean'].values[0]:.4f}")
    print(f"SE: {pct_flu['se'].values[0]:.5f}")
    
    # Flu shot by demographic groups
    print("\n" + "-" * 70)
    print("FLU SHOT PROPORTION BY DEMOGRAPHIC GROUPS")
    print("-" * 70)
    
    print("\nBy Sex:")
    flu_by_sex = design_valid.mean('flu_shot', domain='SEX_LABEL')
    for _, row in flu_by_sex.iterrows():
        print(f"  {row['domain_value']:15s}: {row['mean']:.4f} (SE: {row['se']:.5f})")
    
    print("\nBy Race/Ethnicity:")
    flu_by_race = design_valid.mean('flu_shot', domain='RACE_LABEL')
    for _, row in flu_by_race.iterrows():
        print(f"  {row['domain_value']:15s}: {row['mean']:.4f} (SE: {row['se']:.5f})")
    
    print("\nBy Insurance Status:")
    flu_by_ins = design_valid.mean('flu_shot', domain='INS_LABEL')
    for _, row in flu_by_ins.iterrows():
        print(f"  {row['domain_value']:15s}: {row['mean']:.4f} (SE: {row['se']:.5f})")
    
    # Logistic regression
    print("\n" + "-" * 70)
    print("LOGISTIC REGRESSION: FACTORS ASSOCIATED WITH FLU SHOT")
    print("-" * 70)
    
    # Prepare data for regression (drop missing values)
    reg_data = fyc_sub[fyc_sub['subpop'] == 1].dropna(subset=['flu_shot'])
    
    design_reg = MEPSSurveyDesign(
        data=reg_data,
        strata='VARSTR',
        cluster='VARPSU',
        weights='SAQWT18F'
    )
    
    # Fit logistic regression
    # Reference categories: Male, Hispanic, Any Private
    result = design_reg.glm(
        'flu_shot ~ AGELAST + C(SEX, Treatment(reference=1)) + '
        'C(RACETHX, Treatment(reference=1)) + C(INSCOV18, Treatment(reference=1))',
        family='binomial'
    )
    
    print("\nLogistic Regression Results:")
    print(result.summary())


if __name__ == "__main__":
    main()
