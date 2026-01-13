# MEPS Analysis in Python

This directory contains Python code for analyzing Medical Expenditure Panel Survey (MEPS) data. The code provides parallel implementations to the R, SAS, and Stata examples in this repository.

## Installation

Install the required packages using pip:

```bash
pip install -r requirements.txt
```

Required packages:
- pandas >= 1.3.0
- statsmodels >= 0.13.0
- pyreadstat >= 1.1.0
- numpy >= 1.21.0
- matplotlib >= 3.5.0
- seaborn >= 0.11.0

## Loading MEPS Data

The `utils/meps_loader.py` module provides functions for loading MEPS data in various formats.

### Basic Usage

```python
from utils.meps_loader import load_meps_data

# Load Stata format (recommended for 2017+ data)
data = load_meps_data("C:/MEPS/h224.dta")

# Load SAS V9 format (2017+ data)
data = load_meps_data("C:/MEPS/h224.sas7bdat")

# Load SAS XPORT format (1996-2016 data)
data = load_meps_data("C:/MEPS/h181.ssp")

# Load ASCII format with column specifications
column_specs = [(0, 8, 'DUPERSID'), (8, 12, 'PANEL'), ...]
data = load_meps_data("C:/MEPS/h181.dat", column_specs=column_specs)
```

### File Format Recommendations

| Data Year | Recommended Format | File Extension |
|-----------|-------------------|----------------|
| 2017+     | Stata             | .dta           |
| 2017+     | SAS V9            | .sas7bdat      |
| 1996-2016 | SAS XPORT         | .ssp           |
| Any       | ASCII             | .dat           |

### Downloading MEPS Data

```python
from utils.meps_loader import download_meps_file

# Download 2020 FYC file in Stata format
download_meps_file("h224", file_format="dta", save_dir="C:/MEPS")

# Download 2016 PMED file in SAS XPORT format
download_meps_file("h188a", file_format="ssp", save_dir="C:/MEPS")
```

## Survey Design

MEPS uses a complex survey design that requires proper specification of strata, clusters (PSUs), and weights for valid statistical inference. The `utils/survey_design.py` module provides a wrapper class for survey-weighted analysis.

### Basic Survey Design Setup

```python
from utils.meps_loader import load_meps_data
from utils.survey_design import MEPSSurveyDesign

# Load data
data = load_meps_data("C:/MEPS/h224.dta")

# Create survey design for 2020 data
design = MEPSSurveyDesign(data, year=2020)
```

The `MEPSSurveyDesign` class automatically uses:
- `VARSTR` for strata
- `VARPSU` for clusters (PSUs)
- `PERWTyyF` for weights (where yy is the 2-digit year)

### Custom Weights

For analyses using Self-Administered Questionnaire (SAQ) items, use SAQ weights:

```python
# Use SAQ weights for flu shot analysis
design = MEPSSurveyDesign(data, year=2018, weights='SAQWT18F')
```

### Survey-Weighted Estimates

```python
# Total expenditures
total_results = design.total('TOTEXP20')
print(f"Total: ${total_results['Total'].values[0]:,.0f}")
print(f"SE: ${total_results['SE'].values[0]:,.0f}")

# Mean expenditure
mean_results = design.mean('TOTEXP20')
print(f"Mean: ${mean_results['Mean'].values[0]:,.0f}")
print(f"SE: ${mean_results['SE'].values[0]:.2f}")

# Proportion (for binary variables)
prop_results = design.proportion('HAS_EXPENSE')
print(f"Proportion: {prop_results['Proportion'].values[0]:.4f}")

# Group-by analysis
group_results = design.group_by('TOTEXP20', by='AGECAT', stat='mean')
print(group_results)
```

### Subpopulation Analysis

For subpopulation (domain) analysis, use the `domain` parameter instead of filtering the data:

```python
# CORRECT: Use domain analysis
results = design.mean('TOTEXP20', domain='ADULT', domain_value=1)

# WRONG: Don't filter data before creating survey design
# This produces incorrect standard errors!
# adults = data[data['ADULT'] == 1]
# design = MEPSSurveyDesign(adults, year=2020)
```

### Regression Analysis

```python
# Logistic regression
formula = 'FLUSHOT ~ AGELAST + FEMALE + NH_WHITE + NH_BLACK'
results = design.glm(formula, family='binomial')

# Print coefficients
print(results['params'])

# Print odds ratios
import numpy as np
print(np.exp(results['params']))
```

## Multi-Year Pooling

When pooling data across multiple years, use the Pooled Variance Linkage file for correct variance estimation:

```python
from utils.survey_design import create_pooled_design

# Load and combine data from multiple years
fyc17 = load_meps_data("C:/MEPS/h201.sas7bdat")
fyc18 = load_meps_data("C:/MEPS/h209.sas7bdat")
fyc19 = load_meps_data("C:/MEPS/h216.sas7bdat")

# Standardize variable names and stack
# ... (see exercise_4d.py for full example)

# Create pooled weight
pooled['POOLWT'] = pooled['PERWT'] / 3

# Merge with Pooled Variance Linkage file
linkage = load_meps_data("C:/MEPS/h36u19.sas7bdat")
pooled = pooled.merge(linkage[['DUPERSID', 'STRA9619', 'PSU9619']], on='DUPERSID')

# Create pooled design
design = create_pooled_design(
    pooled,
    years=[2017, 2018, 2019],
    pooled_strata='STRA9619',
    pooled_cluster='PSU9619',
    weight_var='POOLWT'
)
```

## Condition-Event Linking

To analyze expenditures or utilization for specific medical conditions, link the Conditions file to event files using the CLNK file:

```python
# Load files
conditions = load_meps_data("C:/MEPS/h222.sas7bdat")  # Conditions
clnk = load_meps_data("C:/MEPS/h220if1.sas7bdat")    # CLNK
events = load_meps_data("C:/MEPS/h220g.sas7bdat")    # Office-based events
fyc = load_meps_data("C:/MEPS/h224.sas7bdat")        # FYC

# Filter conditions by CCSR code (e.g., mental health)
mental_health = conditions[
    conditions['CCSR1X'].str.startswith('MBD', na=False) |
    conditions['CCSR2X'].str.startswith('MBD', na=False)
]

# Link to events via CLNK
cond_events = mental_health.merge(
    clnk[['DUPERSID', 'CONDIDX', 'EVNTIDX']],
    on=['DUPERSID', 'CONDIDX']
)

# De-duplicate by EVNTIDX (critical!)
cond_events = cond_events.drop_duplicates(subset=['EVNTIDX'])

# Merge with event file
final = cond_events.merge(events, on=['DUPERSID', 'EVNTIDX'])
```

## Directory Structure

```
Python/
├── README.md                      # This file
├── requirements.txt               # Package dependencies
├── utils/
│   ├── __init__.py
│   ├── meps_loader.py            # Data loading utilities
│   └── survey_design.py          # Survey design wrapper
├── workshop_exercises/
│   ├── __init__.py
│   ├── exercise_1a.py            # Expenses by age group
│   ├── exercise_1b.py            # Expenses by service type
│   ├── exercise_2a.py            # Prescribed medicines
│   ├── exercise_3a.py            # Diabetes expenditures
│   ├── exercise_4d.py            # Multi-year pooling
│   ├── exercise_6a.py            # Logistic regression
│   ├── cond_mv_2020.py           # Condition-event linking
│   └── cond_pmed_2020.py         # Condition-medication linking
└── summary_tables_examples/
    ├── __init__.py
    ├── care_access_2017.py       # Care access measures
    ├── care_access_2019.py
    ├── care_quality_2016.py
    ├── care_diabetes_a1c_2016.py # Diabetes care
    ├── cond_expenditures_2015.py # Expenditures by condition
    ├── cond_expenditures_2018.py
    ├── ins_age_2016.py           # Insurance coverage
    ├── pmed_prescribed_drug_2016.py    # Top drugs
    ├── pmed_therapeutic_class_2016.py  # Therapeutic classes
    ├── use_expenditures_2016.py  # Expenditures by service
    ├── use_expenditures_2019.py
    └── use_events_2016.py        # Events by service
```

## Common Patterns

### Pattern 1: Basic Expenditure Analysis

```python
from utils.meps_loader import load_meps_data
from utils.survey_design import MEPSSurveyDesign

# Load data
data = load_meps_data("C:/MEPS/h224.dta")

# Create expense flag
data['HAS_EXP'] = (data['TOTEXP20'] > 0).astype(int)

# Create survey design
design = MEPSSurveyDesign(data, year=2020)

# Calculate estimates
total = design.total('TOTEXP20')
mean = design.mean('TOTEXP20')
pct_with_exp = design.mean('HAS_EXP')

print(f"Total expenditures: ${total['Total'].values[0]/1e9:.1f} billion")
print(f"Mean per person: ${mean['Mean'].values[0]:,.0f}")
print(f"Percent with expense: {pct_with_exp['Mean'].values[0]*100:.1f}%")
```

### Pattern 2: Analysis by Subgroup

```python
import pandas as pd

# Create age categories
data['AGECAT'] = pd.cut(
    data['AGELAST'],
    bins=[-1, 17, 44, 64, 200],
    labels=['0-17', '18-44', '45-64', '65+']
)

# Analyze by age group
for age_cat in ['0-17', '18-44', '45-64', '65+']:
    subset = data[data['AGECAT'] == age_cat].copy()
    design_sub = MEPSSurveyDesign(subset, year=2020)
    mean = design_sub.mean('TOTEXP20')
    print(f"{age_cat}: ${mean['Mean'].values[0]:,.0f}")
```

### Pattern 3: Prescribed Medicines Analysis

```python
# Load PMED and FYC files
pmed = load_meps_data("C:/MEPS/h220a.sas7bdat")
fyc = load_meps_data("C:/MEPS/h224.sas7bdat")

# Aggregate to person level
pers_pmed = pmed.groupby('DUPERSID').agg({
    'RXXP20X': 'sum',
    'RXRECIDX': 'count'
}).reset_index()
pers_pmed.columns = ['DUPERSID', 'PMED_EXP', 'N_FILLS']

# Merge with FYC
pers_pmed = pers_pmed.merge(
    fyc[['DUPERSID', 'VARSTR', 'VARPSU', 'PERWT20F']],
    on='DUPERSID'
)

# Create survey design
design = MEPSSurveyDesign(pers_pmed, year=2020)
```

## Key Variables

### Survey Design Variables
- `VARSTR`: Variance stratum
- `VARPSU`: Variance primary sampling unit
- `PERWTyyF`: Person weight for year yy (e.g., PERWT20F)
- `SAQWTyyF`: SAQ weight for year yy

### Expenditure Variables
- `TOTEXPyy`: Total expenditures
- `TOTSLFyy`: Out-of-pocket payments
- `TOTMCRyy`: Medicare payments
- `TOTMCDyy`: Medicaid payments
- `TOTPRVyy`: Private insurance payments

### Utilization Variables
- `OBTOTVyy`: Office-based visits
- `OPTOTVyy`: Outpatient visits
- `ERTOTyy`: Emergency room visits
- `IPDISyy`: Inpatient discharges
- `RXTOTyy`: Prescription fills

### Condition Coding
- `CCSR1X`, `CCSR2X`, `CCSR3X`: CCSR codes (2016+)
- `CCCODEX`: CCS code (pre-2016)
- `ICD10CDX`: ICD-10 code (2016+)
- `ICD9CODX`: ICD-9 code (pre-2016)

## Resources

- [MEPS Website](https://meps.ahrq.gov/)
- [MEPS Data Files](https://meps.ahrq.gov/mepsweb/data_stats/download_data_files.jsp)
- [MEPS Documentation](https://meps.ahrq.gov/mepsweb/data_stats/download_data_files_documentation.jsp)
- [MEPS GitHub Repository](https://github.com/HHS-AHRQ/MEPS)
