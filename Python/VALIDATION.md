# Validation of the Python ports

Every Python script is compared with its R reference script (same filename, `.R`) run on the
same MEPS public use files. Point estimates and standard errors are compared row by row
(domain x variable); a script passes when all rows match and the maximum relative
difference is below `1e-6`.

## Results

| Script | Data | Output compared | Rows | Max rel. diff (estimate) | Max rel. diff (SE) | Result |
| ------ | ---- | --------------- | ---: | -----------------------: | -----------------: | :----: |
| [care_access_2017.py](summary_tables_examples/care_access_2017.py) | h201 | Number of people, by poverty x reason | 15 | 2.1e-16 | 4.5e-15 | PASS |
|  |  | Percent of people, by poverty x reason | 15 | 2.2e-15 | 1.9e-15 | PASS |
| [care_access_2019.py](summary_tables_examples/care_access_2019.py) | h216 | Number of people, by poverty | 20 | 1.9e-16 | 2.1e-15 | PASS |
|  |  | Percent of people, by poverty | 20 | 2.8e-15 | 3.4e-14 | PASS |
| [care_diabetes_a1c_2016.py](summary_tables_examples/care_diabetes_a1c_2016.py) | h192 | Number of adults, by race x A1c status | 20 | 2.0e-16 | 3.1e-15 | PASS |
|  |  | Percent of adults, by race x A1c status | 20 | 2.1e-15 | 4.6e-15 | PASS |
| [care_quality_2016.py](summary_tables_examples/care_quality_2016.py) | h192 | Number of adults, by insurance x response | 35 | 2.1e-16 | 4.5e-15 | PASS |
|  |  | Percent of adults, by insurance x response | 35 | 2.6e-15 | 1.9e-14 | PASS |
| [cond_expenditures_2015.py](summary_tables_examples/cond_expenditures_2015.py) | h178a, h178d-h, h178if1, h180 | Persons, events, expenditures, by condition | 177 | 2.9e-15 | 4.7e-15 | PASS |
|  |  | Mean expenditure per person, by condition | 59 | 3.9e-15 | 4.0e-15 | PASS |
| [cond_expenditures_2018.py](summary_tables_examples/cond_expenditures_2018.py) | h206a, h206d-h, h206if1, h207 | Persons, events, expenditures, by condition | 246 | 3.9e-15 | 4.3e-15 | PASS* |
|  |  | Mean expenditure per person, by condition | 82 | 3.9e-15 | 3.9e-15 | PASS* |
| [ins_age_2016.py](summary_tables_examples/ins_age_2016.py) | h192 | Number of people, by age group x insurance | 35 | 2.1e-16 | 3.8e-15 | PASS** |
|  |  | Percent of people, by age group x insurance | 35 | 3.7e-15 | 1.4e-14 | PASS** |
| [pmed_prescribed_drug_2016.py](summary_tables_examples/pmed_prescribed_drug_2016.py) | h188a | Persons, purchases, expenditures, by drug name | 1926 | 4.6e-15 | 4.7e-15 | PASS |
| [pmed_therapeutic_class_2016.py](summary_tables_examples/pmed_therapeutic_class_2016.py) | h188a | Persons, purchases, expenditures, by therapeutic class | 54 | 3.3e-15 | 4.4e-15 | PASS |
| [use_events_2016.py](summary_tables_examples/use_events_2016.py) | h192, h188f, h188g | Number of events (OB, OB physician, OP, OP physician) | 4 | 5.3e-16 | 6.1e-16 | PASS |
|  |  | Mean expenditure per event, by source of payment | 20 | 3.7e-15 | 2.6e-15 | PASS |
|  |  | Mean events per person (OB, OB physician) | 2 | 8.0e-16 | 8.8e-16 | PASS |
| [use_expenditures_2016.py](summary_tables_examples/use_expenditures_2016.py) | h192 | Total and mean expenditures, by event type x source of payment | 40 | 2.4e-15 | 4.0e-15 | PASS |
|  |  | Mean out-of-pocket expense among people with an OOP expense | 4 | 1.2e-15 | 1.1e-15 | PASS |
| [use_expenditures_2019.py](summary_tables_examples/use_expenditures_2019.py) | h216 | Mean expenditure per person (total, ER, inpatient), by source of payment | 18 | 2.4e-15 | 3.2e-15 | PASS |
| [use_race_sex_2016.py](summary_tables_examples/use_race_sex_2016.py) | h192 | People, percent with expense, mean expense, by sex x race | 40 | 1.1e-15 | 7.2e-15 | PASS |
|  |  | Mean and median expense per person with an expense, by sex x race | 20 | 8.1e-16 | 5.7e-15 | PASS |

Differences are at floating-point precision, i.e. the Python estimates and SEs reproduce R exactly.

\* The R reference script fails as written (see below); it was compared after the one-line fix.
\*\* The R reference script has a typo (see below); it was compared after the one-character fix.

## Known issues and differences

* <b>CCSR crosswalk has a 4th column.</b> `cond_expenditures_2018` downloads
  `CCSR-ICD10-to-MEPS-collapsed-conditions.csv` from the MEPS website. The file now has a
  `Category Body System` column, so `setNames(c("CCSR", "CCSR_desc", "Condition"))` in
  [cond_expenditures_2018.R](../R/summary_tables_examples/cond_expenditures_2018.R) errors.
  The Python port keeps only the first 3 columns; for validation the R script was run with
  `select(1:3) %>%` inserted before `setNames(...)`. The R script itself is unchanged in this PR.
  (The SAS script renames/drops by column name and is unaffected.)
* <b>Typo in `ins_age_2016.R`.</b> Line `FYC <- read.xport("C:/MEPS/h192.ssp")s` has a stray
  `s` and fails to parse. For validation it was run with the `s` removed; the R script itself
  is unchanged.
* <b>Medians.</b> `samplics` has no design-based quantile with R-compatible SEs, so
  `survey_quantile` implements R `svyquantile`'s defaults directly (weighted inverse-CDF
  quantile; Woodruff interval SE using t quantiles with df = PSUs - strata within the
  domain, as R `degf()` computes on the domain design). Medians and SEs in
  `use_race_sex_2016` match R; a synthetic-design test checks it without MEPS data.
* <b>XPORT zeros.</b> `pandas.read_sas` decodes some zero values in XPORT files as
  `5.4e-79`. In h188a this affected `PERWT16F` for 4,473 records, which created a spurious
  domain (`BROMPHENIRAMINE-PHENYLPROPANOLAMINE`, all-zero weights) that R and SAS omit.
  `read_meps` now maps these values to 0.
* <b>Lonely PSUs.</b> The MEPS analyses above have no singleton strata, so R's
  `survey.lonely.psu = "adjust"` rule is validated separately on a synthetic design with two
  singleton strata in [tests](tests) (totals, means, ratios, domains, subpopulations).
* <b>samplics private API.</b> `survey_estimate` calls `TaylorEstimator._get_point` and
  `_get_variance` directly because `TaylorEstimator.estimate()` raises `ZeroDivisionError`
  when computing the CV of a domain whose estimate is 0 (e.g. a drug with no expenditure).
  `samplics` is pinned to 0.6.1 for this reason.

## Re-running the validation

Requires R with the `survey`, `dplyr`, `tidyr`, `readr`, `haven`, and `foreign` packages, the
Python requirements, and the MEPS files used by the scripts in `<meps_dir>`.

``` bash
cd Python/validation
# 1. Run the R reference script, saving each svyby/svytotal/svymean result to CSV
Rscript run_r_reference.R ../../R/summary_tables_examples/care_access_2019.R ~/MEPS r_out/care_access_2019
# 2. Run the Python port and compare (exit code 0 = PASS)
python compare_to_r.py ../summary_tables_examples/care_access_2019.py r_out/care_access_2019 --meps-dir ~/MEPS
```
