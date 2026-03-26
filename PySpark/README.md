# Analyzing MEPS data using PySpark <!-- omit in toc -->

- [Loading MEPS data](#loading-meps-data)
  - [Data years 2017 and later: SAS V9 files (.sas7bdat)](#data-years-2017-and-later-sas-v9-files-sas7bdat)
  - [Data years 1996-2016: SAS transport files (.ssp)](#data-years-1996-2016-sas-transport-files-ssp)
- [Downloading MEPS data](#downloading-meps-data)
- [Survey-weighted analysis](#survey-weighted-analysis)
- [PySpark examples](#pyspark-examples)
  - [Workshop exercises](#workshop-exercises)
  - [Summary tables examples](#summary-tables-examples)
  - [Older Exercises (1996 to 2006)](#older-exercises-1996-to-2006)

# Setup

## Requirements

Install the required Python packages:

```bash
pip install -r requirements.txt
```

Key dependencies:
- **pyspark** (>=3.4.0): Distributed data processing for ETL operations
- **pandas** (>=1.5.0): Data manipulation (used as intermediate for data loading and survey estimation)
- **samplics** (>=0.4.0): Survey-weighted estimation using Taylor series linearization
- **statsmodels** (>=0.14.0): Survey-weighted logistic regression
- **requests** (>=2.28.0): Downloading MEPS files from the web
- **sas7bdat** (>=2.2.3): Reading SAS data files

## SparkSession

All scripts use a shared utility to create a SparkSession:

```python
from utils.data_loader import get_spark

spark = get_spark()
```

# Loading MEPS data

The `utils/data_loader.py` module provides reusable functions for loading MEPS data files into PySpark DataFrames.

## Data years 2017 and later: SAS V9 files (.sas7bdat)

For data years 2017 and later, the recommended format is `.sas7bdat`. Files are loaded via pandas as an intermediate step:

```python
from utils.data_loader import load_sas7bdat, get_spark

spark = get_spark()
h206b = load_sas7bdat(spark, "C:/MEPS/h206b.sas7bdat")

# View first 10 rows
h206b.show(10)
```

This replaces the SAS code:
```sas
DATA work.h206b;
  SET "C:/MEPS/h206b.sas7bdat";
RUN;
```

## Data years 1996-2016: SAS transport files (.ssp)

For data years 1996-2016, MEPS data is distributed in SAS transport (XPORT) format (`.ssp`):

```python
from utils.data_loader import load_ssp, get_spark

spark = get_spark()
h188b = load_ssp(spark, "C:/MEPS/h188b.ssp")

# View first 10 rows
h188b.show(10)
```

This replaces the SAS code:
```sas
FILENAME in_h188b "C:\MEPS\h188b.ssp";
PROC XCOPY in = in_h188b out = WORK IMPORT;
RUN;
```

# Downloading MEPS data

The `utils/data_loader.py` module includes a function to download MEPS files directly from the MEPS website:

```python
from utils.data_loader import download_meps_file

download_meps_file(
    meps_file="h206b",
    meps_url="https://meps.ahrq.gov/mepsweb/data_files/pufs/h206b/h206bv9.zip",
    local_dir="C:/MEPS"
)
```

# Survey-weighted analysis

## Hybrid approach: PySpark + Python survey libraries

PySpark does not have native complex survey analysis procedures. This migration uses a **hybrid approach**:

1. **PySpark** handles all data loading, cleaning, merging, aggregation, and transformation (the ETL pipeline)
2. **Python survey libraries** (`samplics`, `statsmodels`) handle the survey-weighted estimation after collecting data to the driver via `.toPandas()`

The `utils/survey_utils.py` module provides wrapper functions that mirror the SAS SURVEY procedures:

| SAS Procedure | PySpark/Python Equivalent |
|---|---|
| `PROC SURVEYMEANS ... MEAN` | `survey_utils.survey_mean()` |
| `PROC SURVEYMEANS ... SUM` | `survey_utils.survey_total()` |
| `PROC SURVEYFREQ` | `survey_utils.survey_freq()` |
| `PROC SURVEYLOGISTIC` | `survey_utils.survey_logistic()` |

### Example: Survey-weighted mean

```python
from utils.survey_utils import survey_mean

results = survey_mean(
    spark_df=meps,
    var_cols=["TOTEXP18"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT18F"
)
print(results)
```

This replaces:
```sas
PROC SURVEYMEANS DATA=meps MEAN STDERR;
  STRATUM VARSTR;
  CLUSTER VARPSU;
  WEIGHT PERWT18F;
  VAR TOTEXP18;
RUN;
```

### Domain (subpopulation) analysis

In SAS, `DOMAIN` keeps all observations for correct variance estimation. The PySpark equivalent passes the full dataset with a domain indicator:

```python
results = survey_mean(
    spark_df=meps,           # Full dataset (not filtered!)
    var_cols=["TOTEXP18"],
    stratum_col="VARSTR",
    cluster_col="VARPSU",
    weight_col="PERWT18F",
    domain_col="POVCAT18"    # Domain variable
)
```

This replaces:
```sas
PROC SURVEYMEANS DATA=meps MEAN STDERR;
  STRATUM VARSTR;
  CLUSTER VARPSU;
  WEIGHT PERWT18F;
  VAR TOTEXP18;
  DOMAIN POVCAT18;
RUN;
```

## Format mappings

SAS `PROC FORMAT` value statements are replaced by Python dictionaries in `utils/format_mappings.py`:

```python
from utils.format_mappings import POVERTY_FORMAT, INSURANCE_FORMAT

# Apply format mapping using PySpark
from pyspark.sql import functions as F

df = df.withColumn("POVCAT_label",
    F.when(F.col("POVCAT18") == 1, POVERTY_FORMAT[1])
     .when(F.col("POVCAT18") == 2, POVERTY_FORMAT[2])
     # ... etc
)
```

# PySpark examples

In order to run the example codes, you must download the relevant MEPS files from the MEPS website and save them to your local computer.

## Workshop exercises

Example codes from previous MEPS workshops and webinars are provided in the [workshop_exercises](workshop_exercises) folder:

### 1. National health care expenses <!-- omit in toc -->
[exercise_1a.py](workshop_exercises/exercise_1a.py): National health care expenses by age group, 2016
<br>
[exercise_1b.py](workshop_exercises/exercise_1b.py): National health care expenses by age group and type of service, 2015
<br>
[exercise_1c.py](workshop_exercises/exercise_1c.py): National health care expenses by age group, 2018

### 2. Prescribed medicine purchases <!-- omit in toc -->
[exercise_2a.py](workshop_exercises/exercise_2a.py): Trends in antipsychotics purchases and expenses, 2015
<br>
[exercise_2b.py](workshop_exercises/exercise_2b.py): Purchases and expenses for narcotic analgesics or narcotic analgesic combos, 2016
<br>
[exercise_2c.py](workshop_exercises/exercise_2c.py): Purchases and expenses for narcotic analgesics or narcotic analgesic combos, 2018

### 3. Medical conditions <!-- omit in toc -->
[exercise_3a.py](workshop_exercises/exercise_3a.py): Use and expenditures for persons with diabetes, 2015
<br>
[exercise_3b.py](workshop_exercises/exercise_3b.py): Expenditures for all events associated with diabetes, 2015

### 4. Pooling data files <!-- omit in toc -->
[exercise_4a.py](workshop_exercises/exercise_4a.py): Pooling MEPS FYC files, 2015 and 2016
<br>
[exercise_4b.py](workshop_exercises/exercise_4b.py): Pooling longitudinal files, panels 17-19
<br>
[exercise_4c.py](workshop_exercises/exercise_4c.py): Pooling MEPS FYC files, 2017 and 2018
<br>
[exercise_4d.py](workshop_exercises/exercise_4d.py): Pooling MEPS FYC files, 2017-2019

### 5. Constructing variables <!-- omit in toc -->
[exercise_5a.py](workshop_exercises/exercise_5a.py): Constructing family-level variables from person-level data, 2015
<br>
[exercise_5b.py](workshop_exercises/exercise_5b.py): Constructing insurance status from monthly insurance variables, 2015

### 6. Regression <!-- omit in toc -->
[exercise_6a.py](workshop_exercises/exercise_6a.py): Logistic regression for flu shot uptake, 2018
<br>
[exercise_6b.py](workshop_exercises/exercise_6b.py): Logistic regression for delayed care due to COVID, 2020

### 7. Linking Medical Conditions and Event Files <!-- omit in toc -->
[cond_pmed_2020.py](workshop_exercises/cond_pmed_2020.py): Utilization and expenditures for prescribed medicine purchases for hyperlipidemia, 2020
<br>
[cond_mv_2020.py](workshop_exercises/cond_mv_2020.py): Utilization and expenditures for office-based visits for mental health, 2020

## Summary tables examples

The following codes provided in the [summary_tables_examples](summary_tables_examples) folder re-create selected statistics from the [MEPS-HC Data Tools](https://datatools.ahrq.gov/meps-hc).

### Accessibility and Quality of Care <!-- omit in toc -->

#### Access to Care <!-- omit in toc -->
[care_access_2017.py](summary_tables_examples/care_access_2017.py): Reasons for difficulty receiving needed care, by poverty status, 2017
<br>
[care_access_2019.py](summary_tables_examples/care_access_2019.py): Number and percent of people who did not receive treatment because they couldn't afford it, by poverty status, 2019

#### Diabetes Care <!-- omit in toc -->
[care_diabetes_a1c_2016.py](summary_tables_examples/care_diabetes_a1c_2016.py): Adults with diabetes receiving hemoglobin A1c blood test, by race/ethnicity, 2016

#### Quality of Care <!-- omit in toc -->
[care_quality_2016.py](summary_tables_examples/care_quality_2016.py): Ability to schedule a routine appointment, by insurance coverage, 2016

### Medical conditions <!-- omit in toc -->
[cond_expenditures_2015.py](summary_tables_examples/cond_expenditures_2015.py): Utilization and expenditures by medical condition, 2015 -- Conditions defined by collapsed ICD-9/CCS codes
<br>
[cond_expenditures_2018.py](summary_tables_examples/cond_expenditures_2018.py): Utilization and expenditures by medical condition, 2018 -- Conditions defined by collapsed ICD-10/CCSR codes

### Health Insurance <!-- omit in toc -->
[ins_age_2016.py](summary_tables_examples/ins_age_2016.py): Health insurance coverage by age group, 2016

### Prescribed drugs <!-- omit in toc -->
[pmed_prescribed_drug_2016.py](summary_tables_examples/pmed_prescribed_drug_2016.py): Purchases and expenditures by generic drug name, 2016
<br>
[pmed_therapeutic_class_2016.py](summary_tables_examples/pmed_therapeutic_class_2016.py): Purchases and expenditures by Multum therapeutic class, 2016

### Use, expenditures, and population <!-- omit in toc -->
[use_events_2016.py](summary_tables_examples/use_events_2016.py): Number of events and mean expenditure per event, for office-based and outpatient events, by source of payment, 2016
<br>
[use_expenditures_2016.py](summary_tables_examples/use_expenditures_2016.py): Expenditures for office-based and outpatient visits, by source of payment, 2016
<br>
[use_expenditures_2019.py](summary_tables_examples/use_expenditures_2019.py): Mean expenditure per person, by event type and source of payment, 2019
<br>
[use_race_sex_2016.py](summary_tables_examples/use_race_sex_2016.py): Utilization and expenditures by race and sex, 2016

## Older Exercises (1996 to 2006)

Codes provided in the [older_exercises_1996_to_2006](older_exercises_1996_to_2006) folder include PySpark programs migrated from older SAS programs for analyzing earlier years of MEPS data.

### Estimation examples <!-- omit in toc -->

[E1.py](older_exercises_1996_to_2006/Estimation_examples/E1.py): Person-level estimates (means, proportions, and totals) for healthcare expenditures, 2001
<br>
[E2.py](older_exercises_1996_to_2006/Estimation_examples/E2.py): Average total healthcare expenditures for children ages 0-5, 1996-1999
<br>
[E3.py](older_exercises_1996_to_2006/Estimation_examples/E3.py): Longitudinal estimates of insurance coverage and expenditures, 1999-2000
<br>
[E4.py](older_exercises_1996_to_2006/Estimation_examples/E4.py): Family-level estimates for healthcare expenditures, 2001
<br>
[E5.py](older_exercises_1996_to_2006/Estimation_examples/E5.py): Event-level expenditure estimates for hospital inpatient stays and office-based medical provider visits, 2001
<br>
[E6.py](older_exercises_1996_to_2006/Estimation_examples/E6.py): National health care expenditures by type of service, 2005
<br>
[E7.py](older_exercises_1996_to_2006/Estimation_examples/E7.py): Colonoscopy screening estimates, 2005
<br>
[E8.py](older_exercises_1996_to_2006/Estimation_examples/E8.py): Expenditures for inpatient stays by source of payment, per stay, per diem, with and without surgery, 2005

### Employment examples <!-- omit in toc -->
[EM1.py](older_exercises_1996_to_2006/Employment_examples/EM1.py): Relationship between health status and current main job weekly earnings, 2002
<br>
[EM2.py](older_exercises_1996_to_2006/Employment_examples/EM2.py): Determine how many people working at the beginning of the year changed jobs, 2002

### Linking examples <!-- omit in toc -->
[L1.py](older_exercises_1996_to_2006/Linking_examples/L1.py): Merge the 2001 MEPS full-year file and the 2001 MEPS Jobs file
<br>
[L1A.py](older_exercises_1996_to_2006/Linking_examples/L1A.py): Combine the 2000 and 2001 MEPS Jobs files
<br>
[L2.py](older_exercises_1996_to_2006/Linking_examples/L2.py): Link 2001 MEPS data with 1999 and 2000 NHIS data
<br>
[L3.py](older_exercises_1996_to_2006/Linking_examples/L3.py): Merge 2001 MEPS Office-based Medical Provider Visits file with full-year file
<br>
[L4.py](older_exercises_1996_to_2006/Linking_examples/L4.py): Merge 2001 MEPS Medical Conditions file with full-year file
<br>
[L5.py](older_exercises_1996_to_2006/Linking_examples/L5.py): Merge 2001 MEPS Medical Conditions file with full-year file and various event files

### Miscellaneous examples <!-- omit in toc -->
[M1.py](older_exercises_1996_to_2006/Misc_examples/M1.py): Demonstrates need for weight variables when analyzing MEPS data, 2003
<br>
[M2.py](older_exercises_1996_to_2006/Misc_examples/M2.py): Demonstrates need for using the STRATUM and PSU variables when analyzing MEPS data, 2003
<br>
[M3.py](older_exercises_1996_to_2006/Misc_examples/M3.py): Using ID variables to merge MEPS files, 2003
<br>
[M4.py](older_exercises_1996_to_2006/Misc_examples/M4.py): Illustrates two ways to calculate the number of events associated with conditions, 2003
<br>
[M5.py](older_exercises_1996_to_2006/Misc_examples/M5.py): Demonstrates the difference between two uses of the term "priority condition" in MEPS, 2003
<br>
[M6.py](older_exercises_1996_to_2006/Misc_examples/M6.py): Demonstrates use of the Diabetes Care Supplement (DCS) weight variable, 2003
<br>
[M7.py](older_exercises_1996_to_2006/Misc_examples/M7.py): Person-level prescribed medicine expenditures for persons with at least one PMED event, 2003
<br>
[M8.py](older_exercises_1996_to_2006/Misc_examples/M8.py): Prescribed medicine expenditures associated with specific conditions, 2003
<br>
[M9.py](older_exercises_1996_to_2006/Misc_examples/M9.py): Descriptive statistics of health insurance status and healthcare utilization, 2003
<br>
[M10.py](older_exercises_1996_to_2006/Misc_examples/M10.py): Compares hospital inpatient expenditures from FYC and event files, 2003
<br>
[M11.py](older_exercises_1996_to_2006/Misc_examples/M11.py): Merge parents' employment status variable to children's records, 2002
