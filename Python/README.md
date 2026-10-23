# Analyzing MEPS data using Python <!-- omit in toc -->

- [Setup](#setup)
- [Loading MEPS data](#loading-meps-data)
  - [Data years 2017 and later: SAS V9 files](#data-years-2017-and-later-sas-v9-files)
  - [Data years 1996-2016: SAS transport files](#data-years-1996-2016-sas-transport-files)
- [Automating file download](#automating-file-download)
- [Translating SAS DATA steps and PROCs](#translating-sas-data-steps-and-procs)
- [Survey estimation](#survey-estimation)
- [Python examples](#python-examples)
  - [Workshop exercises](#workshop-exercises)
  - [Summary tables examples](#summary-tables-examples)
  - [Older Exercises (1996 to 2006)](#older-exercises-1996-to-2006)

The Python track mirrors the [SAS](../SAS) track: every SAS program has a Python port with the same folder, file name stem and header comments (description, input files, expected estimates), and the ports reproduce the SAS estimates. Shared helpers live in [meps_utils.py](meps_utils.py). See [VALIDATION.md](VALIDATION.md) for how each port was checked against the committed SAS output and the R track.

# Setup

``` bash
python -m venv meps-venv
source meps-venv/bin/activate        # Windows: meps-venv\Scripts\activate
pip install -r Python/requirements.txt

export MEPS_DIR=~/MEPS               # Windows: set MEPS_DIR=C:\MEPS
python Python/workshop_exercises/exercise_1a/Exercise1a.py
```

`MEPS_DIR` replaces the `C:\MEPS` folder hard-coded in the SAS programs (it defaults to `C:/MEPS`). Scripts can be run from any working directory.

# Loading MEPS data

`read_meps(name, columns=None)` looks for the MEPS file `name` in `MEPS_DIR` (and its sub-folders), downloads it from the MEPS website if it is missing, upper-cases the column names and strips character values:

``` python
from meps_utils import read_meps

h206b = read_meps("h206b")                                   # 2018 Dental Visits
fyc18 = read_meps("h209", ["DUPERSID", "TOTEXP18", "PERWT18F", "VARSTR", "VARPSU"])
print(h206b.head(10))                                        # PROC PRINT (obs=10)
```

## Data years 2017 and later: SAS V9 files

SAS V9 files (`DATA ...; SET "C:/MEPS/h206b.sas7bdat";`) are read with `pandas.read_sas`:

``` python
h206b = pd.read_sas(MEPS_DIR / "h206b.sas7bdat", encoding="latin-1")
```

## Data years 1996-2016: SAS transport files

SAS transport (.ssp) files (`PROC XCOPY ... IMPORT`) are read with `pandas.read_sas(..., format="xport")`. Some early files written on VM/CMS have headers that pandas rejects; `read_meps` then falls back to `pyreadstat.read_xport`. It also maps the tiny non-zero values that IBM floating-point conversion leaves in a few files back to 0:

``` python
h188b = pd.read_sas(MEPS_DIR / "h188b.ssp", format="xport", encoding="latin-1")
```

# Automating file download

`download_meps(name)` downloads and unzips a public use file into `MEPS_DIR`. It tries the SAS V9 zip (`pufs/<name>/<name>v9.zip`), then the SAS transport zip (`pufs/<name>ssp.zip`), then the Stata zip. `read_meps` calls it automatically; pass `download=False` to require a local file. See [meps_files_names.csv](../Quick_Reference_Guides/meps_file_names.csv) for file names.

# Translating SAS DATA steps and PROCs

| SAS | Python (pandas / meps_utils) |
| --- | --- |
| `DATA ...; SET` / `PROC XCOPY` | `read_meps(...)` (`pandas.read_sas`, `format="xport"` for .ssp) |
| `IF ... THEN x = ...; ELSE ...` | `df["X"] = np.where(cond, a, b)`, `np.select([...], [...], default)` |
| `PROC FORMAT` + `FORMAT x fmt.` | dict of labels, ranges as `slice(lo, hi)`: `sas_format(df.X, FMT)`; `pd.Categorical` to order levels |
| `PROC SORT; BY` | `df.sort_values(..., kind="stable")` |
| `MERGE a(IN=A) b(IN=B); BY id; IF A AND B` | `a.merge(b, on="id", how="inner")` (`IF A` -> `how="left"`, no IN= -> `how="outer"`) |
| `SET a b; BY id` (interleave) | `pd.concat([a, b]).sort_values("id", kind="stable")` |
| `FIRST.` / `LAST.` / `RETAIN` accumulators | `groupby().agg()`, `groupby().cumsum()`, `groupby().tail(1)` |
| `PROC MEANS ... CLASS ...; OUTPUT OUT= SUM=` | `df.groupby(class).agg(...)` |
| `PROC FREQ ... / LIST MISSING` | `freq_list(df, cols)` |
| `PROC PRINT` | `show(title, df)` |
| `PROC SURVEYMEANS` (MEAN, SUM, RATIO, QUANTILE) | `surveymeans`, `surveyratio`, `surveyquantile` |
| `PROC SURVEYFREQ` / SUDAAN `PROC CROSSTAB` | `surveyfreq` |
| `PROC SURVEYREG` / `PROC SURVEYLOGISTIC` | `surveyreg(..., family="linear" / "logistic")` |

Non-survey PROCs that SAS runs with a `WEIGHT` statement (e.g. `PROC MEANS`, `PROC TABULATE`, `PROC FREQ` with `WEIGHT`) only produce weighted point estimates, and are ported as weighted sums / `np.average(..., weights=...)`.

# Survey estimation

As in SAS, MEPS estimates must account for the complex survey design to obtain unbiased estimates and correct standard errors:

1. Always use the survey estimators in `meps_utils` (built on [samplics](https://github.com/samplics-org/samplics) Taylor-series linearization), never plain `DataFrame.mean()` or unweighted regression.
2. Always pass the strata (e.g. `VARSTR`), PSU (e.g. `VARPSU`) and appropriate weight (e.g. `PERWT18F`). `strata="VARSTR"` and `cluster="VARPSU"` are the defaults.
3. Do not drop observations before estimating. Use `domain=` for subpopulations: records outside the domain stay in the design (every stratum and PSU is kept), exactly like the SAS `DOMAIN` statement, the R `subset()` of a `svydesign` object and SUDAAN `SUBPOPN`.

For example, total healthcare expenditures in 2018 (`PROC SURVEYMEANS SUM; STRATUM VARSTR; CLUSTER VARPSU; WEIGHT PERWT18F; VAR DVXP18X;`):

``` python
from meps_utils import read_meps, surveymeans, surveyreg

fyc = read_meps("h209")
surveymeans(fyc, "DVXP18X", weight="PERWT18F")                                  # N, SumWgt, Mean, StdErr, Sum, StdDev
surveymeans(fyc, "TOTEXP18", weight="PERWT18F", domain="SEX")                   # DOMAIN SEX
surveyreg(fyc, "TOTEXP18 ~ AGELAST + C(SEX)", weight="PERWT18F", family="linear")
```

`surveyreg` fits the model with `samplics.regression.SurveyGLM` and applies the SAS default `VADJUST=DF` variance adjustment (n-1)/(n-p), so coefficients and standard errors match `PROC SURVEYREG` / `PROC SURVEYLOGISTIC`. `design_df` and `clm` add the SAS design degrees of freedom and t-based confidence limits.

# Python examples

`read_meps` downloads any MEPS file that is not already in `MEPS_DIR`, so the examples can be run without manually downloading data (see [Loading MEPS data](#loading-meps-data)).

## Workshop exercises
Python ports of the example codes from previous MEPS workshops and webinars are provided in the [workshop_exercises](workshop_exercises) folder, using the same folder and file names as the SAS track (e.g. `exercise_1a/Exercise1a.py`). Compare their printed output with the committed SAS output (`*_OUTPUT.TXT`) in [SAS/workshop_exercises](../SAS/workshop_exercises).

### 1. National health care expenses <!-- omit in toc -->
[exercise_1a](workshop_exercises/exercise_1a): National health care expenses by age group, 2016
<br>
[exercise_1b](workshop_exercises/exercise_1b): National health care expenses by age group and type of service, 2015
<br>
[exercise_1c](workshop_exercises/exercise_1c): National health care expenses by age group, 2018
<br>

### 2. Prescribed medicine purchases <!-- omit in toc -->
[exercise_2a](workshop_exercises/exercise_2a): Trends in antipsychotics purchases and expenses, 2015
<br>
[exercise_2b](workshop_exercises/exercise_2b): Purchases and expenses for narcotic analgesics or narcotic analgesic combos, 2016
<br>
[exercise_2c](workshop_exercises/exercise_2c): Purchases and expenses for narcotic analgesics or narcotic analgesic combos, 2018


### 3. Medical conditions <!-- omit in toc -->
[exercise_3a](workshop_exercises/exercise_3a): Use and expenditures for persons with diabetes, 2015
<br>
[exercise_3b](workshop_exercises/exercise_3b): Expenditures for all events associated with diabetes, 2015

### 4. Pooling data files <!-- omit in toc -->
[exercise_4a](workshop_exercises/exercise_4a): Pooling MEPS FYC files, 2015 and 2016: Out-of-pocket expenditures for unisured persons ages 26-30 with high income
<br>
[exercise_4b](workshop_exercises/exercise_4b): Pooling longitudinal files, panels 17-19
<br>
[exercise_4c](workshop_exercises/exercise_4c): Pooling MEPS FYC files, 2017 and 2018: People with joint pain, using JTPAIN31 for 2017 and JTPAIN31_M18 for 2018
<br>
[exercise_4d](workshop_exercises/exercise_4d): Pooling MEPS FYC files, 2017-2019: People with joint pain, using [Pooled Linkage Variance file](https://meps.ahrq.gov/mepsweb/data_stats/download_data_files_detail.jsp?cboPufNumber=HC-036) for correct standard error calculation (required when pooling before and after 2019)
<br>

### 5. Constructing variables <!-- omit in toc -->
[exercise_5a](workshop_exercises/exercise_5a): Constructing family-level variables from person-level data, 2015
<br>
[exercise_5b](workshop_exercises/exercise_5b): Constructing insurance status from monthly insurance variables, 2015

### 6. Regression <!-- omit in toc -->
[exercise_6a](workshop_exercises/exercise_6a): Logistic regression to identify demographic factors associated with receiving a flu shot in 2018 (using SAQ population)
<br>
[exercise_6b](workshop_exercises/exercise_6b): Logistic regression for persons that delayed medical care because of COVID, 2020
<br>

### 7. Linking Medical Conditions and Event Files  <!-- omit in toc -->
[cond_pmed_2020.py](workshop_exercises/cond_pmed_2020.py): Utilization and expenditures for prescribed medicine purchases for hyperlipidemia, 2020
<br>
[cond_mv_2020.py](workshop_exercises/cond_mv_2020.py): Utilization and expenditures for office-based visits for mental health, 2020

## Summary tables examples

The following codes provided in the [summary_tables_examples](summary_tables_examples) folder re-create selected statistics from the [MEPS-HC Data Tools](https://datatools.ahrq.gov/meps-hc). These example codes are written under the assumption that the .ssp files are saved in the local directory "C:/MEPS/". However, you can customize the programs to point to an alternate directory.


### Accessibility and Quality of Care  <!-- omit in toc -->

#### Access to Care <!-- omit in toc -->
[care_access_2017.py](summary_tables_examples/care_access_2017.py):
Reasons for difficulty receiving needed care, by poverty status, 2017
<br>
[care_access_2019.py](summary_tables_examples/care_access_2019.py):
Number and percent of people who did not receive treatment because they couldn't afford it, by poverty status, 2019

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
[use_expenditures_2019.py](summary_tables_examples/use_expenditures_2019.py): Mean expenditure per person, by event type and source of payment, 2019.
<br>
[use_race_sex_2016.py](summary_tables_examples/use_race_sex_2016.py): Utilization and expenditures by race and sex, 2016



## Older Exercises (1996 to 2006)

Python ports of the older SAS programs are provided in the [older_exercises_1996_to_2006](older_exercises_1996_to_2006) folder. Compare their output with the SAS output (`.pdf`) committed next to each program in [SAS/older_exercises_1996_to_2006](../SAS/older_exercises_1996_to_2006). These programs read the SAS transport (.ssp) files; see [VALIDATION.md](VALIDATION.md) for files that have since been re-released under new names.

### Estimation examples <!-- omit in toc -->

[E1](older_exercises_1996_to_2006/Estimation_examples/E1):
Person-level estimates (means, proportions, and totals) for healthcare expenditures, 2001
<br>
[E2](older_exercises_1996_to_2006/Estimation_examples/E2): Average total healthcare expenditures for children ages 0-5, 1996-1999
<br>
[E3](older_exercises_1996_to_2006/Estimation_examples/E3): Longitudinal estimates of insurance coverage and expenditures, 1999-2000
<br>
[E4](older_exercises_1996_to_2006/Estimation_examples/E4): Family-level estimates for healthcare expenditures, 2001
<br>
[E5](older_exercises_1996_to_2006/Estimation_examples/E5): Event-level expenditure estimates for  hospital inpatient stays and office-based medical provider visits, 2001
<br>
[E6](older_exercises_1996_to_2006/Estimation_examples/E6): National health care expenditures by type of service, 2005 (Statistical Brief #193)
<br>
[E7](older_exercises_1996_to_2006/Estimation_examples/E7): Colonoscopy screening estimates, 2005 (Statistical Brief #188)
<br>
[E8](older_exercises_1996_to_2006/Estimation_examples/E8): Expenditures for inpatient stays by source of payment, per stay, per diem, with and without surgery, 2005

### Employment examples <!-- omit in toc -->
[EM1](older_exercises_1996_to_2006/Employment_examples/EM1): Relationship between health status and current main job weekly earnings, 2002
<br>
[EM2](older_exercises_1996_to_2006/Employment_examples/EM2): Determine how many people working at the beginning of the year changed jobs, 2002

### Linking examples <!-- omit in toc -->
[L1](older_exercises_1996_to_2006/Linking_examples/L1): Merge the 2001 MEPS full-year file and the 2001 MEPS Jobs file
<br>
[L1A](older_exercises_1996_to_2006/Linking_examples/L1A): Combine the 2000 and 2001 MEPS Jobs files
<br>
[L2](older_exercises_1996_to_2006/Linking_examples/L2): Link 2001 MEPS data with 1999 and 2000 NHIS data
<br>
[L3](older_exercises_1996_to_2006/Linking_examples/L3): Merge 2001 MEPS Office-based Medical Provider Visits file with full-year file
<br>
[L4](older_exercises_1996_to_2006/Linking_examples/L4): Merge 2001 MEPS Medical Conditions file with full-year file
<br>
[L5](older_exercises_1996_to_2006/Linking_examples/L5): Merge 2001 MEPS Medical Conditions file with full-year file and various event files

### Miscellaneous examples <!-- omit in toc -->
[M1](older_exercises_1996_to_2006/Misc_examples/M1): Demonstrates need for weight variables when analyzing MEPS data, 2005
<br>
[M2](older_exercises_1996_to_2006/Misc_examples/M2): Demonstrates need for using the STRATUM and PSU variables when analyzing MEPS data, 2005
<br>
[M3](older_exercises_1996_to_2006/Misc_examples/M3): Using ID variables to merge MEPS files, 2005
<br>
[M4](older_exercises_1996_to_2006/Misc_examples/M4): Illustrates two ways to calculate the number of events associated with conditions. (1) using the evNUM variables on the CONDITIONS file. (2) using the number of matches between the CONDITIONS file and the CLINK file, 2003
<br>
[M5](older_exercises_1996_to_2006/Misc_examples/M5): Demonstrates the difference between two uses of the term "priority condition" in MEPS, 2005
<br>
[M6](older_exercises_1996_to_2006/Misc_examples/M6): Demonstrates use of the Diabetes Care Supplement (DCS) weight variable, 2006
<br>
[M7](older_exercises_1996_to_2006/Misc_examples/M7): Person-level prescribed medicine expenditures for persons with at least one PMED event, 2003
<br>
[M8](older_exercises_1996_to_2006/Misc_examples/M8): Prescribed medicine expenditures associated with cancer conditions, 2005
<br>
[M9](older_exercises_1996_to_2006/Misc_examples/M9): Descriptive statistics of health insurance status and healthcare utilization, 2005
<br>
[M10](older_exercises_1996_to_2006/Misc_examples/M10): Compares hospital inpatient expenditures (facility, physician, total) for stays that do and do not include facility expenditures for the preceding emergency room visit, 2003
<br>
[M11](older_exercises_1996_to_2006/Misc_examples/M11): Merge parents' employment status variable  to children's records, 2006
