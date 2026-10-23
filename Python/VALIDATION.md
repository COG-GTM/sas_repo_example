# Validation of the Python track

Every Python port was run against the MEPS public use files and its printed output was compared with the committed SAS output (`*_OUTPUT.TXT` for workshop exercises, `.pdf` for older exercises) and, for the summary tables and the two condition-linkage workshop programs, with the output of the equivalent R program (R `survey` 4.1.1) run on the same data.

Environment: Python 3.12, packages in [requirements.txt](requirements.txt); R 4.x with `survey`, `haven`, `dplyr`, `tidyr`.

## How the comparison was done

1. Run each script with `MEPS_DIR` pointing to the downloaded files and save its output.
2. Extract every number with at least 4 significant digits from the reference output and check that it also appears (to 4 significant digits, allowing for x100 percent scaling) in the Python output. These token counts are a coarse screen only: reference listings also contain record dumps, IDs, dates and statistics the port does not print (e.g. SAS CLMs, variable lists), so a count below 100% does not by itself mean a mismatch.
3. For every analysis, the key survey estimates (means, totals, percentages, regression coefficients and their standard errors) were compared side by side with the reference. The notes column records the result of that comparison.

## Summary tables (reference: R `survey` output)

| Script | Runs | Tokens matched | Notes |
| --- | --- | --- | --- |
| care_access_2017 | yes | 64/70 | Estimates and SEs match R |
| care_access_2019 | yes | 86/92 | Estimates and SEs match R |
| care_diabetes_a1c_2016 | yes | 65/66 | Estimates and SEs match R |
| care_quality_2016 | yes | 112/122 | Estimates and SEs match R |
| cond_expenditures_2015 | yes | 472/472 | Exact |
| cond_expenditures_2018 | yes | n/a | R reference program fails locally (tibble assignment error in the R script), so this port was checked against its SAS program logic only |
| ins_age_2016 | yes | 64/80 | Estimates and SEs match R |
| pmed_prescribed_drug_2016 | yes | 3858/3858 | Exact |
| pmed_therapeutic_class_2016 | yes | 108/108 | Exact |
| use_events_2016 | yes | 45/50 | Means/totals and SEs match R; median SEs differ slightly (see below) |
| use_expenditures_2016 | yes | 88/88 | Exact |
| use_expenditures_2019 | yes | 34/36 | Means/totals and SEs match R; median SEs differ slightly (see below) |
| use_race_sex_2016 | yes | 99/125 | Estimates and SEs match R |

## Workshop exercises (reference: committed SAS `*_OUTPUT.TXT`)

| Script | Runs | Tokens matched | Notes |
| --- | --- | --- | --- |
| exercise_1a/Exercise1a | yes | 44/52 | Totals, percents, means and SEs match SAS |
| exercise_1b/Exercise1b | yes | 312/370 | Match |
| exercise_1c/Exercise1c | yes | 52/109 | Match; unmatched tokens are SAS CLMs / CVs not printed by the port |
| exercise_2a/Exercise2a | yes | 108/210 | Match; unmatched tokens are PROC PRINT record dumps |
| exercise_2b/Exercise2b | yes | 129/194 | Match; unmatched tokens are PROC PRINT record dumps |
| exercise_2c/Exercise2c | yes | 66/72 | Match |
| exercise_3a/Exercise3a | yes | 85/100 | Match |
| exercise_3b/Exercise3b | yes | 457/684 | Match; unmatched tokens are record dumps |
| exercise_4a/Exercise4a | yes | 179/190 | Match |
| exercise_4b/Exercise4b | yes | 119/122 | Match |
| exercise_4c/Exercise4c | yes | 118/134 | Match |
| exercise_4d/Exercise4 | yes | 46/48 | Match |
| exercise_5a/Exercise5a | yes | 15/164 | FAMSIZE 2.205817 (0.019593), FAMOOP 1328.594367 (32.955419), FAMINC 64940 (953.000134) match exactly; the SAS listing is mostly a family-ID sample dump |
| exercise_5b/Exercise5b | yes | 672/860 | Match |
| exercise_6a/Exercise6 | yes | 22/61 | SURVEYLOGISTIC coefficients and SEs match to all printed digits (e.g. Intercept -2.1417 (0.0814), AGELAST 0.0313 (0.00121)) |
| exercise_6b/exercise6 | yes | 96/211 | SURVEYLOGISTIC coefficients and SEs match to all printed digits |
| cond_mv_2020 | yes | 38/105 (vs R) | No committed SAS output; checked against R `cond_mv_2020.R`: totals, means and SEs match (expenditure total SE 4,437,433,004 (requires keeping missing analysis values in the DOMAIN design) |
| cond_pmed_2020 | yes | 59/89 (vs R) | No committed SAS output; checked against R `cond_pmed_2020.R`: totals, means and SEs match exactly. R sets `survey.adjust.domain.lonely = TRUE`, which changes the POVCAT20 domain SEs (e.g. 0.2973 vs 0.2901); with that option off, as in SAS, R and Python agree to all digits |

## Older exercises, 1996-2006 (reference: committed SAS `.pdf`)

| Script | Runs | Tokens matched | Notes |
| --- | --- | --- | --- |
| E1 | yes | 88/153 | Means, proportions, totals and SEs match |
| E2 | yes | 201/303 | Match; HC-036 (1996-2001 pooled variance file) is no longer published, its replacement `h36u24` is used, which changes some pooled SEs in the last digit |
| E3 | yes | 58/115 | Match (e.g. UNINS00 domain means 3.467562 (0.264600), 76.924987 (1.552855)); the re-released HC-058 renames LONGWTP4/YRINDP4 to LONGWT/YEARIND and the port renames them back |
| E4 | yes | 154/183 | Match |
| E5 | yes | 16/50 | Event means match (inpatient 6628.807015 (263.362555), office 114.168495 (1.677911)); remaining tokens are CLMs and listing values |
| E6 | yes | 90/130 | Match |
| E7 | yes | 425/739 | Match |
| E8 | yes | 81/174 | Match |
| EM1 | yes | 61/166 | Close, not exact: HC-062 (2002 FY, `PERWT02P`) is no longer published; the port falls back to its replacement HC-070 (`PERWT02F`) |
| EM2 | yes | 58/95 | Close, not exact (same HC-070 fallback): changed jobs 30.00% vs 30.1% in SAS |
| L1 | yes | 190/242 | Weighted counts, SEs and percents match |
| L1A | yes | 382/451 | Match |
| L2 | no | - | Needs the NHIS 1999/2000 person files and MEPS-NHIS link file (`nhisper99.dat`, `nhisper00.dat`, `nhmep01x.dat`), which are not public use downloads; the script stops with a message if they are not in `NHIS_DIR` |
| L3 | yes | 84/138 | Match |
| L4 | yes | 266/350 | Match |
| L5 | yes | 92/156 | Match |
| M1 | yes | 6/41 | Unweighted/weighted race percents match |
| M2 | yes | 8/54 | SRS and complex-design totals and SEs match |
| M3 | yes | 396/476 | Match |
| M4 | yes | - | The committed PDF contains only the program and the BOTH/JUSTA/JUSTB log line (80708/25571/0); no tabulated output to compare. HC-077I1 is now published as HC-077IF1 |
| M5 | yes | 170/268 | Match |
| M6 | yes | 38/82 | SURVEYFREQ weighted percents and SEs match (e.g. 73.7031 (1.2143)) |
| M7 | yes | 4/11 | PMED expenditure totals match; the PDF has few numeric values |
| M8 | yes | 850/1061 | Match; HC-094I1 is now published as HC-094IF1 |
| M9 | yes | 78/126 | Domain means and SEs match |
| M10 | yes | 12/13 | Match |
| M11 | yes | 619/736 | Match (e.g. BOTH PARENTS WORK 40.7313 (0.9405)) |

## Known differences

- **Quantile SEs**: medians match SAS/R; their SEs use Woodruff's method and can differ from R `svyquantile` (`qrule`/interval defaults) in the last digits.
- **Regression p-values**: `surveyreg` reports Wald chi-square p-values; SAS SURVEYLOGISTIC prints t-based p-values with design df, so p-values can differ in the 3rd decimal (coefficients and SEs match).
- **Re-released historical files**: HC-036, HC-062, HC-077I1 and HC-094I1 are no longer available under their original names. The ports document and use the replacement files (see notes above).
