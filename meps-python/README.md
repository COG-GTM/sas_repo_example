# MEPS Python Package

Python package for analyzing Medical Expenditure Panel Survey (MEPS) data. Migrated from the COG-GTM/sas_repo_example repository which contained R, SAS, and Stata implementations.

## Overview

This package provides Python equivalents for analyzing MEPS Household Component data, including:

- **Data Loading**: Functions to load MEPS data files from various formats (.dta, .sas7bdat, .ssp, .xlsx) and directly from the MEPS website
- **Survey Design**: Classes to define complex survey designs with stratification, clustering, and weights
- **Analysis Functions**: Survey-weighted statistics including totals, means, proportions, quantiles, and regression

The package is designed to produce nationally representative estimates with correct standard errors by properly accounting for MEPS's complex sample design.

## Installation

```bash
pip install -e .
```

Or install dependencies directly:

```bash
pip install -r requirements.txt
```

### Dependencies

- pandas>=1.5.0
- numpy>=1.23.0
- pyreadstat>=1.2.0
- statsmodels>=0.14.0
- requests>=2.28.0
- openpyxl>=3.0.0
- scipy>=1.9.0

## Quick Start

```python
from meps import read_meps, MEPSSurveyDesign, svymean, svytotal

# Load 2018 Full-Year Consolidated file
fyc = read_meps(year=2018, type='FYC')

# Define survey design
design = MEPSSurveyDesign(
    data=fyc,
    id_var='VARPSU',
    strata_var='VARSTR',
    weight_var='PERWT18F'
)

# Calculate weighted mean of total expenditures
result = svymean(design, 'TOTEXP18')
print(result)

# Calculate weighted total
result = svytotal(design, 'TOTEXP18')
print(result)
```

## Core Modules

### Data Loading (`meps.data_loading`)

```python
from meps import read_meps, download_meps_file

# Load data by year and type
fyc = read_meps(year=2020, type='FYC')
conditions = read_meps(year=2020, type='COND')
pmed = read_meps(year=2020, type='PMED')
clnk = read_meps(year=2020, type='CLNK')

# Load by file name
ob2020 = read_meps(file='h220g')

# Load event files by type
ob = read_meps(year=2020, type='OB')  # Office-based
op = read_meps(year=2020, type='OP')  # Outpatient
er = read_meps(year=2020, type='ER')  # Emergency room
ip = read_meps(year=2020, type='IP')  # Inpatient

# Load pooled linkage file for multi-year analysis
linkage = read_meps(type='Pooled linkage')

# Download and save locally
filepath = download_meps_file('h224', file_format='dta', save_dir='/path/to/save')
```

### Survey Design (`meps.survey_design`)

```python
from meps import MEPSSurveyDesign

# Define survey design (equivalent to R's svydesign)
design = MEPSSurveyDesign(
    data=fyc,
    id_var='VARPSU',      # PSU variable
    strata_var='VARSTR',  # Strata variable
    weight_var='PERWT18F', # Weight variable
    nest=True,            # PSUs nested within strata
    lonely_psu='adjust'   # Handle lonely PSUs (like R's survey.lonely.psu='adjust')
)

# Subset design for domain analysis
adults = design.subset(fyc['AGELAST'] >= 18)
```

### Analysis Functions (`meps.analysis`)

```python
from meps import svytotal, svymean, svyby, svyglm, svyquantile

# Population totals
result = svytotal(design, ['TOTEXP18', 'TOTSLF18'])

# Weighted means
result = svymean(design, 'TOTEXP18')

# Estimates by group
result = svyby(design, 'TOTEXP18', 'POVCAT18', svymean)

# Logistic regression
result = svyglm(
    design,
    formula='flu_shot ~ AGELAST + as.factor(SEX) + as.factor(RACETHX)',
    family='quasibinomial'
)

# Quantiles (e.g., median)
result = svyquantile(design, 'TOTEXP18', quantiles=[0.25, 0.5, 0.75])
```

### Utilities (`meps.utils`)

```python
from meps.utils import (
    recode_factor,
    filter_ccsr,
    filter_ccsr_pattern,
    merge_condition_event,
    aggregate_to_person,
    pool_weights,
    combine_years
)

# Recode categorical variables
fyc['poverty'] = recode_factor(
    fyc['POVCAT18'],
    {1: "Poor", 2: "Near-poor", 3: "Low income", 4: "Middle income", 5: "High income"}
)

# Filter conditions by CCSR code
hyperlipidemia = filter_ccsr(conditions, 'END010')

# Filter by CCSR pattern
mental_health = filter_ccsr_pattern(conditions, 'MBD|FAC002|FAC007')

# Merge conditions with events via CLNK
merged = merge_condition_event(conditions, clnk, events, event_type=1)

# Pool multiple years
pooled = combine_years([fyc17, fyc18, fyc19], years=[2017, 2018, 2019])
pooled = pool_weights(pooled, 'perwt', n_years=3)
```

## Platform Equivalents

| R Function | SAS Procedure | Stata Command | Python Function |
|-----------|---------------|---------------|-----------------|
| `svydesign()` | `STRATA/CLUSTER/WEIGHT` | `svyset` | `MEPSSurveyDesign()` |
| `svytotal()` | `PROC SURVEYMEANS SUM` | `svy: total` | `svytotal()` |
| `svymean()` | `PROC SURVEYMEANS MEAN` | `svy: mean` | `svymean()` |
| `svyby()` | `PROC SURVEYMEANS DOMAIN` | `svy: mean, over()` | `svyby()` |
| `svyglm()` | `PROC SURVEYREG/SURVEYLOGISTIC` | `svy: regress/logit` | `svyglm()` |
| `svyquantile()` | `PROC SURVEYMEANS QUANTILE` | `svy: mean` | `svyquantile()` |
| `subset()` | `DOMAIN` | `subpop()` | `design.subset()` |

## Examples

### Care Access Analysis (2017)

```python
from meps import read_meps, MEPSSurveyDesign, svymean, svyby

# Load data
fyc = read_meps(year=2017, type='FYC')

# Create indicator variables
fyc['delay_MD'] = ((fyc['MDUNAB42'] == 1) | (fyc['MDDLAY42'] == 1)).astype(int)
fyc['afford_MD'] = ((fyc['MDDLRS42'] == 1) | (fyc['MDUNRS42'] == 1)).astype(int)

# Define survey design
design = MEPSSurveyDesign(fyc, weight_var='PERWT17F')

# Subset to eligible population
sub = design.subset((fyc['ACCELI42'] == 1) & (fyc['delay_MD'] == 1))

# Calculate estimates by poverty status
result = svyby(sub, 'afford_MD', 'POVCAT17', svymean)
```

### Condition-Event Linkage (2020)

```python
from meps import read_meps, MEPSSurveyDesign, svytotal
from meps.utils import filter_ccsr_pattern

# Load files
ob = read_meps(year=2020, type='OB')
cond = read_meps(year=2020, type='COND')
clnk = read_meps(year=2020, type='CLNK')
fyc = read_meps(year=2020, type='FYC')

# Filter to mental health conditions
mental_health = filter_ccsr_pattern(cond, 'MBD|FAC002|FAC007')

# Link conditions to events
clnk_ob = clnk[clnk['EVENTYPE'] == 1]
merged = mental_health.merge(clnk_ob, on=['DUPERSID', 'CONDIDX'])
merged = merged.drop_duplicates('EVNTIDX')
merged = merged.merge(ob, on=['DUPERSID', 'EVNTIDX'])

# Merge with FYC for complete survey structure
merged = merged.merge(fyc[['DUPERSID', 'VARSTR', 'VARPSU', 'PERWT20F']], on='DUPERSID')

# Calculate estimates
design = MEPSSurveyDesign(merged, weight_var='PERWT20F')
result = svytotal(design, 'OBXP20X')
```

### Multi-Year Pooling (2017-2019)

```python
from meps import read_meps, MEPSSurveyDesign, svymean
import pandas as pd

# Load data
fyc17 = read_meps(year=2017, type='FYC')
fyc18 = read_meps(year=2018, type='FYC')
fyc19 = read_meps(year=2019, type='FYC')
linkage = read_meps(type='Pooled linkage')

# Standardize variable names
fyc17 = fyc17.rename(columns={'PERWT17F': 'perwt', 'TOTEXP17': 'totexp'})
fyc18 = fyc18.rename(columns={'PERWT18F': 'perwt', 'TOTEXP18': 'totexp'})
fyc19 = fyc19.rename(columns={'PERWT19F': 'perwt', 'TOTEXP19': 'totexp'})

# Stack and create pooled weight
pooled = pd.concat([fyc17, fyc18, fyc19])
pooled['poolwt'] = pooled['perwt'] / 3

# Merge with linkage file for correct variance estimation
pooled = pooled.merge(linkage[['DUPERSID', 'PANEL', 'PSU9619', 'STRA9619']], 
                      on=['DUPERSID', 'PANEL'])

# Use PSU9619/STRA9619 for pooled analysis
design = MEPSSurveyDesign(
    pooled,
    id_var='PSU9619',
    strata_var='STRA9619',
    weight_var='poolwt'
)

result = svymean(design, 'totexp')
```

## Important Notes

### Survey Variance Estimation

This package implements Taylor series linearization for variance estimation, properly accounting for MEPS's complex survey design with stratification and clustering. The `lonely_psu='adjust'` option (default) handles strata with single PSUs, equivalent to R's `options(survey.lonely.psu='adjust')`.

### Weight Variable Selection

Different analyses require different weight variables:

- **Person-level analysis**: Use `PERWTyyF` (e.g., `PERWT18F`)
- **SAQ variables** (e.g., flu shot): Use `SAQWTyyF` (e.g., `SAQWT18F`)
- **Pooled analysis**: Divide weight by number of years

### File Format Compatibility

- **2017+**: Use `.dta` (Stata) or `.sas7bdat` (SAS V9) files
- **1996-2016**: Use `.ssp` (SAS XPORT) files
- The `read_meps()` function automatically selects the appropriate format

### Domain Analysis

When analyzing subpopulations, use `design.subset()` to maintain the full survey structure for proper variance estimation. Do not filter the data before creating the survey design.

## File Structure

```
meps-python/
├── meps/
│   ├── __init__.py
│   ├── data_loading.py
│   ├── survey_design.py
│   ├── analysis.py
│   └── utils.py
├── examples/
│   ├── workshop_exercises/
│   │   ├── cond_mv_2020.py
│   │   ├── exercise_3d.py
│   │   └── exercise_4a.py
│   └── summary_tables_examples/
│       ├── care_access_2017.py
│       ├── care_access_2019.py
│       ├── care_quality_2016.py
│       ├── ins_age_2016.py
│       ├── pmed_prescribed_drug_2016.py
│       └── use_expenditures_2016.py
├── tests/
├── requirements.txt
├── setup.py
└── README.md
```

## Resources

- [MEPS Website](https://meps.ahrq.gov/)
- [MEPS Data Files](https://meps.ahrq.gov/mepsweb/data_stats/download_data_files.jsp)
- [MEPS Data Tools](https://datatools.ahrq.gov/meps-hc)
- [Original R/SAS/Stata Repository](https://github.com/COG-GTM/sas_repo_example)

## License

MIT License
