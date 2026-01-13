# MEPS Analysis with Python

This directory contains Python code for analyzing Medical Expenditure Panel Survey (MEPS) data. The code provides equivalent functionality to the SAS, R, and Stata implementations in this repository.

## Installation

Install the required packages using pip:

```bash
pip install -r requirements.txt
```

Or install packages individually:

```bash
pip install pandas numpy statsmodels pyreadstat matplotlib seaborn scipy
```

## Directory Structure

```
Python/
├── README.md                    # This file
├── requirements.txt             # Package dependencies
├── utils/
│   ├── __init__.py             # Package exports
│   ├── meps_loader.py          # Data loading utilities
│   └── survey_design.py        # Survey design wrapper class
├── workshop_exercises/          # Workshop exercise examples
│   ├── exercise_1a.py          # Basic expenses by age, 2016
│   ├── exercise_1b.py          # Expenses by service type, 2015
│   ├── exercise_1c.py          # Basic expenses by age, 2018
│   ├── exercise_2a.py          # Antipsychotics purchases, 2015
│   ├── exercise_2b.py          # Narcotic analgesics, 2016
│   ├── exercise_2c.py          # Narcotic analgesics, 2018
│   ├── exercise_3a.py          # Diabetes expenditures, 2015
│   ├── exercise_3b.py          # Diabetes-linked events, 2015
│   ├── exercise_4a.py          # Pooling FYC 2015-2016
│   ├── exercise_4b.py          # Pooling longitudinal files
│   ├── exercise_4c.py          # Pooling FYC 2017-2018
│   ├── exercise_4d.py          # Pooling with variance linkage
│   ├── exercise_5a.py          # Family-level variables
│   ├── exercise_5b.py          # Insurance status construction
│   ├── exercise_6a.py          # Logistic regression: flu shot
│   ├── exercise_6b.py          # Logistic regression: COVID delayed care
│   ├── cond_pmed_2020.py       # Condition-PMED linking
│   └── cond_mv_2020.py         # Condition-office visit linking
└── summary_tables_examples/     # Summary table replication examples
    ├── care_access_2017.py     # Access to care, 2017
    ├── care_access_2019.py     # Affordability, 2019
    ├── care_diabetes_a1c_2016.py  # Diabetes A1c testing
    ├── care_quality_2016.py    # Quality of care
    ├── ins_coverage_2016.py    # Insurance coverage
    ├── pmed_prescribed_drug_2016.py  # Prescribed drugs by name
    ├── pmed_therapeutic_class_2018.py  # Drugs by therapeutic class
    ├── cond_expenditures_2018.py  # Condition expenditures
    └── use_expenditures_2018.py  # Use and expenditures
```

## Loading MEPS Data

The `meps_loader` module supports multiple file formats used by MEPS:

### SAS Transport (.ssp) - 1996-2016 data

```python
from utils.meps_loader import load_meps_data

# Load 2016 Full-Year Consolidated file
fyc = load_meps_data("C:/MEPS/h192.ssp")
```

### SAS V9 (.sas7bdat) - 2017+ data

```python
# Load 2020 Full-Year Consolidated file
fyc = load_meps_data("C:/MEPS/h224.sas7bdat")
```

### Stata (.dta) - 2017+ data

```python
# Load 2018 Full-Year Consolidated file
fyc = load_meps_data("C:/MEPS/h209.dta")
```

### ASCII (.dat) with programming statements

```python
from utils.meps_loader import load_meps_ascii

# Load ASCII file with column specifications
fyc = load_meps_ascii(
    "C:/MEPS/h192.dat",
    col_specs=[
        ('DUPERSID', 0, 10),
        ('AGELAST', 10, 13),
        ('SEX', 13, 14),
        # ... additional columns
    ]
)
```

### Downloading MEPS data

```python
from utils.meps_loader import download_meps_file, get_meps_file_url

# Get URL for a MEPS file
url = get_meps_file_url('h209', 'dta')

# Download file to local directory
filepath = download_meps_file('h209', 'dta', 'C:/MEPS/')
```

## Survey Design Setup

MEPS uses a complex survey design that must be accounted for in all analyses. The `MEPSSurveyDesign` class wraps the survey design specification:

```python
from utils.survey_design import MEPSSurveyDesign

# Create survey design for 2018 data
design = MEPSSurveyDesign(
    data=fyc,
    strata='VARSTR',
    cluster='VARPSU',
    weights='PERWT18F'
)
```

### Critical Survey Variables

All MEPS analyses must include these three survey design variables:

| Variable | Description |
|----------|-------------|
| `VARSTR` | Variance estimation stratum |
| `VARPSU` | Variance estimation primary sampling unit |
| `PERWTyyF` | Person-level weight (where yy is 2-digit year) |

### Weight Variables by File Type

| File Type | Weight Variable | Example |
|-----------|-----------------|---------|
| Full-Year Consolidated | PERWTyyF | PERWT18F |
| Prescribed Medicines | PERWTyyF | PERWT18F |
| Medical Conditions | PERWTyyF | PERWT18F |
| Office-Based Visits | PERWTyyF | PERWT18F |
| Self-Administered Questionnaire | SAQWTyyF | SAQWT18F |
| Diabetes Care Supplement | DIABWyyF | DIABW18F |

## Common Analysis Patterns

### Totals

```python
# Total health care expenditures
result = design.total('TOTEXP18')
print(f"Total: ${result['total'].values[0]:,.0f}")
print(f"SE: ${result['se'].values[0]:,.0f}")
```

### Means

```python
# Mean expenditure per person
result = design.mean('TOTEXP18')
print(f"Mean: ${result['mean'].values[0]:,.2f}")
print(f"SE: ${result['se'].values[0]:,.2f}")
```

### Proportions

```python
# Create indicator variable
fyc['has_expense'] = (fyc['TOTEXP18'] > 0).astype(int)

# Proportion with any expense
result = design.mean('has_expense')
print(f"Proportion: {result['mean'].values[0]:.4f}")
```

### Domain/Subgroup Analysis

```python
# Mean expenditure by age group
fyc['AGE_CAT'] = pd.cut(fyc['AGELAST'], bins=[0, 18, 45, 65, 200],
                        labels=['<18', '18-44', '45-64', '65+'])
result = design.mean('TOTEXP18', domain='AGE_CAT')
```

### Subpopulation Analysis

```python
# Analysis for adults only (maintaining full sample for variance estimation)
design_adults = design.subset(fyc['AGELAST'] >= 18)
result = design_adults.mean('TOTEXP18')
```

### Regression Analysis

```python
# Linear regression
result = design.glm('TOTEXP18 ~ AGELAST + C(SEX) + C(RACETHX)')
print(result.summary())

# Logistic regression
result = design.glm('has_expense ~ AGELAST + C(SEX)', family='binomial')
print(result.summary())
```

## Pooling Multiple Years

When pooling multiple years of MEPS data, weights must be adjusted:

```python
from utils.survey_design import create_pooled_design

# Pool 2017-2018 data
fyc17 = load_meps_data("C:/MEPS/h201.dta")
fyc18 = load_meps_data("C:/MEPS/h209.dta")

# Add year identifier
fyc17['YEAR'] = 2017
fyc18['YEAR'] = 2018

# Combine and adjust weights
pooled = pd.concat([fyc17, fyc18], ignore_index=True)
pooled['POOLWT'] = pooled.apply(
    lambda x: x['PERWT17F'] / 2 if x['YEAR'] == 2017 else x['PERWT18F'] / 2,
    axis=1
)

# Create pooled design
design = MEPSSurveyDesign(
    data=pooled,
    strata='VARSTR',
    cluster='VARPSU',
    weights='POOLWT'
)
```

### Pooling Across 2019 (CAPI Redesign)

When pooling data that spans 2019, use the Pooled Variance Linkage file for correct variance estimation:

```python
# Load pooled variance linkage file
linkage = load_meps_data("C:/MEPS/h36brr.dta")

# Merge with pooled data
pooled = pooled.merge(linkage[['DUPERSID', 'PANEL', 'STRA9619', 'PSU9619']],
                      on=['DUPERSID', 'PANEL'], how='left')

# Use linkage variables for variance estimation
design = MEPSSurveyDesign(
    data=pooled,
    strata='STRA9619',
    cluster='PSU9619',
    weights='POOLWT'
)
```

## Linking Conditions to Events

To analyze expenditures for specific medical conditions, link the Conditions file to event files using the CLNK (Condition-Event Link) file:

```python
# Load files
pmed = load_meps_data("C:/MEPS/h220a.sas7bdat")  # Prescribed Medicines
cond = load_meps_data("C:/MEPS/h222.sas7bdat")  # Conditions
clnk = load_meps_data("C:/MEPS/h220if1.sas7bdat")  # CLNK

# Filter conditions (e.g., diabetes: CCSR codes starting with 'END002' or 'END003')
diabetes_cond = cond[cond['CCSR1X'].str.startswith('END00', na=False)]

# Link to events via CLNK
diabetes_events = diabetes_cond.merge(clnk, on=['DUPERSID', 'CONDIDX'])

# De-duplicate by EVNTIDX to avoid double-counting
diabetes_events = diabetes_events.drop_duplicates(subset=['DUPERSID', 'EVNTIDX'])

# Link to PMED file
diabetes_pmed = diabetes_events.merge(pmed, 
    left_on=['DUPERSID', 'EVNTIDX'],
    right_on=['DUPERSID', 'LINKIDX'])
```

## SAS to Python Translation Reference

| SAS | Python |
|-----|--------|
| `DATA step` | pandas DataFrame operations |
| `PROC MEANS` | `design.mean()`, `df.describe()` |
| `PROC FREQ` | `pd.crosstab()`, `df.value_counts()` |
| `PROC SURVEYMEANS` | `design.mean()`, `design.total()` |
| `PROC SURVEYFREQ` | `design.proportion()` |
| `PROC SURVEYREG` | `design.glm()` |
| `PROC SURVEYLOGISTIC` | `design.glm(family='binomial')` |
| `PROC SQL` | pandas merge/join, `df.query()` |
| `%MACRO` | Python functions |
| `WHERE` | `df[df['var'] == value]` |
| `IF-THEN-ELSE` | `np.where()`, `df.loc[]` |

## Resources

- [MEPS Homepage](https://meps.ahrq.gov/)
- [MEPS-HC Data Files](https://meps.ahrq.gov/mepsweb/data_stats/download_data_files.jsp)
- [MEPS-HC Documentation](https://meps.ahrq.gov/mepsweb/data_stats/download_data_files_documentation.jsp)
- [statsmodels Documentation](https://www.statsmodels.org/)
- [pandas Documentation](https://pandas.pydata.org/docs/)
