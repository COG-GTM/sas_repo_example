# Validation of the Python ports

Every Python script is compared with its R reference script (same filename, `.R`) run on the
same MEPS public use files. Point estimates and standard errors are compared row by row
(domain x variable); a script passes when all rows match and the maximum relative
difference is below `1e-6`.

## Results

| Script | Data | Output compared | Rows | Max rel. diff (estimate) | Max rel. diff (SE) | Result |
| ------ | ---- | --------------- | ---: | -----------------------: | -----------------: | :----: |
| [care_access_2019.py](summary_tables_examples/care_access_2019.py) | h216 | Number of people, by poverty | 20 | 1.9e-16 | 2.1e-15 | PASS |
| | | Percent of people, by poverty | 20 | 2.8e-15 | 3.4e-14 | PASS |
| [cond_expenditures_2018.py](summary_tables_examples/cond_expenditures_2018.py) | h206a, h206d-h, h206if1, h207 | Persons, events, expenditures, by condition | 246 | 3.9e-15 | 4.3e-15 | PASS* |
| | | Mean expenditure per person, by condition | 82 | 3.9e-15 | 3.9e-15 | PASS* |
| [pmed_prescribed_drug_2016.py](summary_tables_examples/pmed_prescribed_drug_2016.py) | h188a | Persons, purchases, expenditures, by drug name | 1926 | 4.6e-15 | 4.7e-15 | PASS |

Differences are at floating-point precision, i.e. the Python estimates and SEs reproduce R exactly.

\* The R reference script fails as written (see below); it was compared after the one-line fix.

## Known issues and differences

* <b>CCSR crosswalk has a 4th column.</b> `cond_expenditures_2018` downloads
  `CCSR-ICD10-to-MEPS-collapsed-conditions.csv` from the MEPS website. The file now has a
  `Category Body System` column, so `setNames(c("CCSR", "CCSR_desc", "Condition"))` in
  [cond_expenditures_2018.R](../R/summary_tables_examples/cond_expenditures_2018.R) errors.
  The Python port keeps only the first 3 columns; for validation the R script was run with
  `select(1:3) %>%` inserted before `setNames(...)`. The R script itself is unchanged in this PR.
  (The SAS script renames/drops by column name and is unaffected.)
* <b>XPORT zeros.</b> `pandas.read_sas` decodes some zero values in XPORT files as
  `5.4e-79`. In h188a this affected `PERWT16F` for 4,473 records, which created a spurious
  domain (`BROMPHENIRAMINE-PHENYLPROPANOLAMINE`, all-zero weights) that R and SAS omit.
  `read_meps` now maps these values to 0.
* <b>Lonely PSUs.</b> The pilot MEPS analyses have no singleton strata, so R's
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
