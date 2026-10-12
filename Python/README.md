# Analyzing MEPS data using Python <!-- omit in toc -->

- [Installing Python packages](#installing-python-packages)
- [Loading MEPS data](#loading-meps-data)
  - [Data years 2017 and later: SAS V9 and Stata](#data-years-2017-and-later-sas-v9-and-stata)
  - [Data years 1996-2016: SAS XPORT format](#data-years-1996-2016-sas-xport-format)
- [Translating SAS DATA steps and procedures](#translating-sas-data-steps-and-procedures)
- [Survey estimation in Python](#survey-estimation-in-python)
  - [Domains and subpopulations](#domains-and-subpopulations)
  - [Lonely PSUs](#lonely-psus)
- [Validation](#validation)
- [Python examples](#python-examples)
  - [Summary tables examples](#summary-tables-examples)
  - [Workshop exercises](#workshop-exercises)

The Python track mirrors the [R](../R), [SAS](../SAS), and [Stata](../Stata) folders: each
`.py` script has the same name as the corresponding `.sas`/`.R` script and reproduces
its estimates. Shared loading and survey-estimation code lives in
[meps_utils.py](meps_utils.py).

# Installing Python packages

> Note: Python 3.10 or later is recommended.

``` bash
pip install -r Python/requirements.txt
```

| Package      | Used for |
| ------------ | -------- |
| `pandas`     | Loading SAS/Stata files (`read_sas`, `read_stata`) and data manipulation |
| `numpy`      | Numerical arrays |
| `samplics`   | Design-based (Taylor-linearization) survey estimation |
| `pyreadstat` | Optional; faster reader for `.sas7bdat`/`.dta` and SAS formats catalogs |
| `pytest`     | Running `Python/tests` |

> <b>Note:</b> `samplics` 0.6.1 prints a `FutureWarning` that the project is archived in favor
> of its successor `svy`. It is pinned here because it is the library these examples were
> validated with; all calls to it are isolated in `meps_utils.survey_estimate`.

# Loading MEPS data

The raw MEPS public use files are <b>not</b> included in this repository. Download them from
the [MEPS website](https://meps.ahrq.gov/mepsweb/data_stats/download_data_files.jsp), unzip
them, and save them to a local folder. The examples look for files in `C:/MEPS` by default;
set the `MEPS_DIR` environment variable (or pass `meps_dir` to `main()`) to use another folder:

``` bash
MEPS_DIR=~/MEPS python Python/summary_tables_examples/care_access_2019.py
```

`meps_utils.read_meps` picks the right `pandas` reader from the file extension, upper-cases
column names, and decodes SAS character columns:

| File type          | Function                                        | Example |
| ------------------ | ----------------------------------------------- | ------- |
| SAS V9 (.sas7bdat) | `pd.read_sas(path, format="sas7bdat")`          | `read_meps("C:/MEPS/h216.sas7bdat")` |
| SAS XPORT (.ssp)   | `pd.read_sas(path, format="xport")`             | `read_meps("C:/MEPS/h188a.ssp")` |
| SAS CPORT (.ssp)   | (none)                                          | N/A -- use .sas7bdat or .dta |
| Stata (.dta)       | `pd.read_stata(path)`                           | `read_meps("C:/MEPS/h216.dta")` |

## Data years 2017 and later: SAS V9 and Stata

``` python
from meps_utils import meps_path, read_meps

fyc2019 = read_meps(meps_path("h216.sas7bdat"))   # SAS: data fyc; set "C:\MEPS\h216.sas7bdat";
```

> <b> IMPORTANT! </b> As in R, the SAS transport (.ssp) versions of most 2017+ files were
> created with the SAS CPORT engine and cannot be read in Python. Use the .sas7bdat or .dta files.

## Data years 1996-2016: SAS XPORT format

``` python
rx2016 = read_meps(meps_path("h188a.ssp"))   # SAS: proc xcopy in = h188a out = WORK IMPORT;
```

`pandas` decodes some IBM-float zeros in XPORT files as `5.4e-79` instead of `0`
(e.g. `PERWT16F` in h188a); `read_meps` sets these to `0`, matching SAS and R.

# Translating SAS DATA steps and procedures

| SAS | Python (pandas) |
| --- | --------------- |
| `afford_MD = (AFRDCA42 = 1);` | `df["afford_MD"] = (df["AFRDCA42"] == 1).astype(int)` |
| `if domain = 0 and PERWT19F = 0 then PERWT19F = 1;` | `df.loc[(df.domain == 0) & (df.PERWT19F == 0), "PERWT19F"] = 1` |
| `PROC FORMAT` / `format POVCAT19 POVCAT.;` | `df["poverty"] = df["POVCAT19"].map(POVERTY)` (dict), optionally `pd.Categorical(..., ordered=True)` |
| `PROC SORT` + `PROC MEANS; BY ...; OUTPUT sum= n= mean=` | `df.groupby([...], as_index=False).agg(x=("X", "sum"), n=("X", "size"), w=("W", "mean"))` |
| `PROC TRANSPOSE` (wide to long, e.g. `CCSR1X-CCSR3X`) | `df.melt(id_vars=[...], value_vars=[...])` |
| `DATA ...; MERGE a b; BY id;` | `a.merge(b, on="id", how="outer")` (`how="left"` for `if in_a`) |
| `SET a b;` (stacking) | `pd.concat([a, b], ignore_index=True)` |
| `PROC SORT NODUPKEY; BY ...;` | `df.drop_duplicates([...])` |
| `PROC FREQ` (QC tables) | `df.groupby([...]).size()` |

# Survey estimation in Python

To get unbiased estimates and correct standard errors, always use the survey design:
strata (`VARSTR`), PSUs (`VARPSU`), and the appropriate weight (e.g. `PERWT19F`).
`meps_utils.survey_estimate` wraps `samplics.TaylorEstimator` and is the Python
equivalent of SAS `PROC SURVEYMEANS` and R `svytotal`/`svymean`/`svyratio`/`svyby`:

``` python
from meps_utils import survey_estimate

# SAS:  proc surveymeans data = h206b sum; stratum VARSTR; cluster VARPSU;
#         weight PERWT18F; var DVXP18X; run;
# R:    svytotal(~DVXP18X, design = svydesign(id = ~VARPSU, strata = ~VARSTR,
#         weights = ~PERWT18F, data = dn2018, nest = TRUE))
survey_estimate(dn2018, "DVXP18X", weight="PERWT18F", stat="total")
```

| Argument | Meaning | SAS | R |
| -------- | ------- | --- | - |
| `stat="total"` / `"mean"` / `"ratio"` | statistic (`ratio` needs `denominator=`) | `SUM` / `MEAN` / `RATIO` | `svytotal` / `svymean` / `svyratio` |
| `stratum="VARSTR"`, `psu="VARPSU"`, `weight=` | design | `STRATA`, `CLUSTER`, `WEIGHT` | `svydesign(strata=, id=, weights=, nest=TRUE)` |
| `domain="poverty"` (or a list) | estimates by group | `DOMAIN poverty` | `svyby(by = ~poverty)` |
| `subpop=df["domain"] == 1` | analytic subpopulation | `DOMAIN domain('1')` | `subset(design, domain == 1)` |

The result is a tidy DataFrame with one row per domain and variable
(`[*domain, variable, estimate, se]`); `wide_estimates()` pivots it to the `svyby` layout.
PSUs are nested within strata (R `nest = TRUE`).

## Domains and subpopulations

<b>Do not subset the data</b> (no `df[df.x == 1]` before estimating) -- just as SAS users
should not use `BY`/`WHERE`. Use `domain=`/`subpop=` instead. Records outside a domain
get a zero contribution but every stratum and PSU stays in the variance calculation;
dropping rows can remove PSUs from the design and understate standard errors (see
`test_domain_estimation_keeps_full_design` in [tests](tests)). Domains whose records all
have zero weight are omitted, as in SAS and R.

## Quantiles (medians)

`meps_utils.survey_quantile` is the equivalent of R `svyquantile` (and
`svyby(..., FUN = svyquantile)`) and SAS `PROC SURVEYMEANS ... MEDIAN`. It uses the same
defaults as R `survey` 4.x: the quantile is the inverse of the weighted empirical CDF
(`qrule = "math"`), and the SE is derived from a Woodruff confidence interval
(half-width / t quantile, with degrees of freedom = PSUs - strata within the domain).
`domain=` and `subpop=` work as in `survey_estimate`.

``` python
survey_quantile(fyc, "TOTEXP16", weight="PERWT16F", quantile=0.5,
                domain=["sex", "race"], subpop=fyc["TOTEXP16"] > 0)
```

## Lonely PSUs

All R examples set `options(survey.lonely.psu = "adjust")`. `survey_estimate` reproduces
this by default (`lonely_psu="adjust"`): a stratum with a single PSU contributes the
squared PSU score total, centered at zero. `lonely_psu="remove"` skips those strata
(the SAS default) and `lonely_psu="fail"` raises an error (the `samplics` default).

# Validation

Each port is validated against the output of its R reference script; see
[VALIDATION.md](VALIDATION.md) for results, known differences, and how to re-run the checks.
[tests/test_meps_utils.py](tests/test_meps_utils.py) checks `survey_estimate` against R
`survey` output (including lonely PSUs, subpopulations, and ratios) on a synthetic design
and runs without MEPS data:

``` bash
python -m pytest Python/tests
```

# Python examples

In order to run the example codes, you must download the relevant MEPS files from the MEPS
website and save them to your local computer (see [Loading MEPS data](#loading-meps-data)).
Each script prints its results and returns them from `main()`.

## Summary tables examples

The following codes in [summary_tables_examples](summary_tables_examples) re-create selected
statistics from the [MEPS-HC Data Tools](https://datatools.ahrq.gov/meps-hc).

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
[use_expenditures_2019.py](summary_tables_examples/use_expenditures_2019.py): Mean expenditure per person, by event type and source of payment, 2019
<br>
[use_race_sex_2016.py](summary_tables_examples/use_race_sex_2016.py): Utilization and expenditures by race and sex, 2016

## Workshop exercises

Ports of the [SAS workshop exercises](../SAS/workshop_exercises) will be added to
[workshop_exercises](workshop_exercises).
