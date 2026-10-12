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

## Workshop exercises

Workshop ports are compared with their R reference (where one exists; R and SAS exercise
numbers differ, see [README](README.md#workshop-exercises)) and with every estimate and SE
printed in the SAS `*_OUTPUT.TXT` listing. The printed SAS values are transcribed in
[validation/sas_reference](validation/sas_reference); a SAS value passes when the Python value
rounds to it at the printed precision.

| Script | R reference | R outputs (rows) | Max rel. diff est / SE vs R | SAS values reproduced |
| ------ | ----------- | ---------------- | --------------------------- | --------------------- |
| [Exercise1a.py](workshop_exercises/exercise_1a/Exercise1a.py) | exercise_1a.R | 5 (7) | 1.6e-15 / 1.4e-14 | 10/10 |
| [Exercise1b.py](workshop_exercises/exercise_1b/Exercise1b.py) | -- | -- | -- | 96/96 |
| [Exercise1c.py](workshop_exercises/exercise_1c/Exercise1c.py) | exercise_1b.R | 6 (9), incl. medians | 3.8e-15 / 6.8e-15 | 14/14 |
| [Exercise2a.py](workshop_exercises/exercise_2a/Exercise2a.py) | -- | -- | -- | 32/32 |
| [Exercise2b.py](workshop_exercises/exercise_2b/Exercise2b.py) | exercise_2a.R** | 2 (8) | 1.2e-15 / 1.5e-15 | 16/16 |
| [Exercise2c.py](workshop_exercises/exercise_2c/Exercise2c.py) | exercise_2b.R* | 2 (8) | 1.2e-15 / 1.6e-15 | 16/16 |
| [Exercise3a.py](workshop_exercises/exercise_3a/Exercise3a.py) | -- | -- | -- | 36/36 |
| [Exercise3b.py](workshop_exercises/exercise_3b/Exercise3b.py) | -- | -- | -- | 332/332 |
| [Exercise4a.py](workshop_exercises/exercise_4a/Exercise4a.py) | exercise_3a.R | 1 (1) | 4.7e-16 / 5.3e-16 | 2/2 |
| [Exercise4b.py](workshop_exercises/exercise_4b/Exercise4b.py) | exercise_3b.R | 1 (3) | 6.1e-16 / 1.8e-15 | 6/6 |
| [Exercise4c.py](workshop_exercises/exercise_4c/Exercise4c.py) | exercise_3c.R** | 2 (6) | 3.7e-15 / 1.5e-14 | 12/12 |
| [Exercise4.py](workshop_exercises/exercise_4d/Exercise4.py) | exercise_3d.R* | 2 (6) | 4.1e-15 / 1.8e-14 | 12/12 |
| [Exercise5a.py](workshop_exercises/exercise_5a/Exercise5a.py) | -- | -- | -- | 6/6 |
| [Exercise5b.py](workshop_exercises/exercise_5b/Exercise5b.py) | -- | -- | -- | 88/88 |
| [Exercise6.py](workshop_exercises/exercise_6a/Exercise6.py) | exercise_4a.R* | 2 (10), incl. logistic model | 5.6e-09 / 3.5e-06 | 16/20 (see below) |
| [exercise6.py](workshop_exercises/exercise_6b/exercise6.py) | exercise_4b.R** | 6 (39), incl. 3 logistic models | 1.7e-10 / 3.3e-07 | 63/78 (see below) |
| [cond_mv_2020.py](workshop_exercises/cond_mv_2020/cond_mv_2020.py) | cond_mv_2020.R** | 8 (8) | 2.1e-15 / 5.3e-15 | no SAS version |
| [cond_pmed_2020.py](workshop_exercises/cond_pmed_2020/cond_pmed_2020.py) | cond_pmed_2020.R* | 4 (19) | 2.9e-15 / 3.5e-15 | no SAS version |
| [ggplot_example.py](workshop_exercises/ggplot_example/ggplot_example.py) | ggplot_example.R | 3 (15) | 1.4e-15 / 1.8e-14 | no SAS version |

All survey means, totals, ratios and medians match R to floating-point precision. Logistic
regression coefficients match R to < 1e-8 and SEs to < 4e-6 (relative), so they are compared
with `--tol 1e-5`; R's `glm` stops iterating at a relative deviance change of 1e-8, so its
coefficients are themselves only converged to about that level.

\* The R script needed a fix to run (see below); compared after the fix.
\** The R script runs only after attaching packages its `library()` lines leave commented out
(`MEPS`, `dplyr`, ...), which [run_r_reference.R](validation/run_r_reference.R) does.

### Workshop known issues and differences

* <b>SAS `PROC SURVEYLOGISTIC` SEs (Exercise6a/6b).</b> SAS applies its default variance
  adjustment `VADJUST=DF`, multiplying the covariance by (n-1)/(n-p). R `svyglm` (and
  `survey_glm`) do not. For Exercise6a (n = 19,171, p = 9) this changes SEs by 0.02%, which
  shifts 4 of the 9 printed SEs by one unit in the last digit; scaling the Python SEs by
  sqrt((n-1)/(n-p)) reproduces all of them. In Exercise6b SAS also stops at its default
  convergence criterion (GCONV=1e-8), so 5 coefficients differ from R/Python in the 4th
  decimal (e.g. uninsured: SAS -0.6933, R and Python -0.69353). The Python values match R.
* <b>Pooled linkage file (Exercise4 / `exercise_3d.R`).</b> The scripts use the 1996-2019
  linkage file (`STRA9619`, `PSU9619`), which MEPS no longer distributes; the current file
  `h36u23` (1996-2023) has `STRA9623`/`PSU9623`. The Python port uses the new variables and
  reproduces the SAS 1996-2019 results exactly; the R script was compared with the variable
  names updated.
* <b>`exercise_2b.R`</b> calls `nrow(fyc)` but the data frame is `fyc18`; compared after the fix.
* <b>`exercise_4a.R`</b> errors in `case_when()` (`TRUE ~ ADFLST42` mixes labelled and double
  types); compared with `TRUE ~ as.numeric(ADFLST42)`.
* <b>`cond_pmed_2020.R`</b> prints all estimates, then errors on an undefined object `nts` at the
  end. It also sets `options(survey.adjust.domain.lonely = TRUE)`, which inflates the variance
  of POVCAT domains with a single PSU in a stratum; `survey_estimate(...,
  adjust_domain_lonely=True)` reproduces this (unit-tested against R in [tests](tests)).
* <b>Exercise numbering.</b> SAS has no counterpart to `exercise_2a.R`'s 2015 antipsychotics R
  version and R has none for SAS Exercise1b, 2a, 3a, 3b, 5a, 5b; those ports are validated
  against the SAS listings only.
* <b>Figures.</b> `ggplot_example.py` draws the bar chart with matplotlib (optional
  dependency); only the plotted percentages are validated.

## Re-running the validation

Requires R with the `survey`, `dplyr`, `tidyr`, `readr`, `haven`, and `foreign` packages, the
Python requirements, and the MEPS files used by the scripts in `<meps_dir>`.

``` bash
cd Python/validation
# 1. Run the R reference script, saving each svyby/svytotal/svymean result to CSV
Rscript run_r_reference.R ../../R/summary_tables_examples/care_access_2019.R ~/MEPS r_out/care_access_2019
# 2. Run the Python port and compare (exit code 0 = PASS)
python compare_to_r.py ../summary_tables_examples/care_access_2019.py r_out/care_access_2019 --meps-dir ~/MEPS
# 3. Workshop exercises: also compare with the values printed in the SAS listing
python compare_to_sas.py ../workshop_exercises/exercise_1a/Exercise1a.py sas_reference/Exercise1a.csv --meps-dir ~/MEPS
```

R scripts that load data with `read_MEPS()` need the [MEPS R package](https://github.com/HHS-AHRQ/MEPS)
installed; `run_r_reference.R` attaches it automatically.
