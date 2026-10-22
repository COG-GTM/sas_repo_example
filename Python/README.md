# Analyzing MEPS data using Python <!-- omit in toc -->

This folder mirrors the [SAS](../SAS) and [R](../R) tracks: every SAS program under
`SAS/summary_tables_examples`, `SAS/workshop_exercises` and
`SAS/older_exercises_1996_to_2006` has a Python port at the same relative path
(e.g. `SAS/workshop_exercises/exercise_6a/Exercise6.sas` →
`Python/workshop_exercises/exercise_6a/Exercise6.py`). Each port keeps the
original SAS header (purpose and input files) as its leading comment block.

- [Installing Python packages](#installing-python-packages)
- [Loading MEPS data](#loading-meps-data)
  - [Data years 2017 and later: SAS V9 (.sas7bdat)](#data-years-2017-and-later-sas-v9-sas7bdat)
  - [Data years 1996-2016: SAS XPORT (.ssp)](#data-years-1996-2016-sas-xport-ssp)
  - [Automating file download](#automating-file-download)
- [Survey analysis in Python](#survey-analysis-in-python)
  - [Domains, not subsets](#domains-not-subsets)
  - [Regression](#regression)
- [SAS to Python mapping](#sas-to-python-mapping)
- [Running the examples](#running-the-examples)

# Installing Python packages

Python 3.10+ is required. From the repository root:

``` bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r Python/requirements.txt
```

| Package | Used for |
|---|---|
| `pandas`, `numpy` | data steps, `PROC SORT`/`MEANS`/`FREQ`/`TRANSPOSE` equivalents |
| `pyreadstat` | reading `.sas7bdat` (and older XPORT files pandas rejects) |
| `samplics` | design-based means, totals, ratios, proportions, quantiles (`PROC SURVEYMEANS`/`SURVEYFREQ`) |
| `statsmodels` | survey-weighted GLM / regression (`PROC SURVEYLOGISTIC`/`SURVEYREG`) |
| `requests` | downloading MEPS public use files |

Shared helpers live in [`meps_utils.py`](meps_utils.py); every example imports it.

# Loading MEPS data

MEPS public use files (PUFs) are available from the
[MEPS website](https://meps.ahrq.gov/mepsweb/data_stats/download_data_files.jsp).
The examples look for files in `C:/MEPS` by default; set the `MEPS_DIR`
environment variable to use another folder.

## Data years 2017 and later: SAS V9 (.sas7bdat)

``` python
import pandas as pd
import pyreadstat

# pandas
h209 = pd.read_sas("C:/MEPS/h209.sas7bdat", format="sas7bdat", encoding="latin-1")

# pyreadstat (faster, can read a subset of columns)
h209, meta = pyreadstat.read_sas7bdat("C:/MEPS/h209.sas7bdat", usecols=["DUPERSID", "PERWT18F", "VARSTR", "VARPSU"])
```

## Data years 1996-2016: SAS XPORT (.ssp)

``` python
h60 = pd.read_sas("C:/MEPS/h60.ssp", format="xport", encoding="latin-1")
```

`meps_utils.read_meps()` wraps both readers, upper-cases column names, strips
character values, and fixes two XPORT quirks: true zeros that pandas decodes as
~5.4e-79, and some older headers that pandas refuses (it then uses
`pyreadstat.read_xport`).

## Automating file download

`meps_utils.load_meps(name)` reads `MEPS_DIR/<name>.sas7bdat` or `.ssp`, and
downloads it first if missing. The download is plain `requests` + `zipfile`:

``` python
import io, zipfile, requests

url = "https://meps.ahrq.gov/mepsweb/data_files/pufs/h209/h209v9.zip"   # 2017+: SAS V9
# url = "https://meps.ahrq.gov/mepsweb/data_files/pufs/h60ssp.zip"       # 1996-2016: XPORT
resp = requests.get(url, timeout=300)
resp.raise_for_status()
with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
    zf.extractall("C:/MEPS")
```

``` python
from meps_utils import load_meps
h209 = load_meps("h209", columns=["DUPERSID", "TOTEXP18", "PERWT18F", "VARSTR", "VARPSU"])
```

# Survey analysis in Python

MEPS has a complex survey design. To get unbiased estimates and correct
standard errors (see [SAS/README.md](../SAS/README.md#sas-survey-procedures)):

1. Always use design-based estimators.
2. Always use the strata (`VARSTR`), cluster/PSU (`VARPSU`), and the appropriate
   weight (`PERWTyyF`, `SAQWTyyF`, `DIABWyyF`, ...).
3. Do not drop records to analyze a subgroup; use a **domain**.

`meps_utils` wraps `samplics.TaylorEstimator` (Taylor linearization,
with-replacement PSUs nested within strata, as SAS and R `svydesign(nest=TRUE)`):

``` python
from meps_utils import load_meps, svy_mean, svy_total, svy_freq

h209 = load_meps("h209", columns=["TOTEXP18", "SEX", "PERWT18F", "VARSTR", "VARPSU"])

svy_total(h209, "TOTEXP18", weight="PERWT18F")                 # PROC SURVEYMEANS SUM
svy_mean(h209, "TOTEXP18", weight="PERWT18F", domain="SEX")    # ... MEAN; DOMAIN SEX
svy_freq(h209, "SEX", weight="PERWT18F")                       # PROC SURVEYFREQ
```

`strata`/`psu` default to `VARSTR`/`VARPSU` and can be overridden (e.g.
`VARSTR01`/`VARPSU01`, or `STRA9619`/`PSU9619` for pooled years). Strata with a
single PSU contribute zero variance, as in SAS.

## Domains, not subsets

``` python
from meps_utils import add_domain

# SAS: if <condition> then domain = 1; else do; domain = 2; SAQWT19F = 1; end;
df = add_domain(df, (df["AGELAST"] >= 18) & (df["SAQWT19F"] > 0), "SAQWT19F")
res = svy_mean(df, "delay_MD", weight="SAQWT19F", domain="domain")
res[res["domain"] == 1]
```

All records stay in the design; only the in-domain rows are reported.

## Regression

`svy_glm` fits the model with `statsmodels` and computes the design-based
sandwich variance (equivalent to R `svyglm`, SAS `SURVEYREG`/`SURVEYLOGISTIC`):

``` python
import statsmodels.api as sm
from meps_utils import svy_glm

fit = svy_glm("FLUSHOT ~ C(SEX) + AGELAST", df, weight="SAQWT18F", family=sm.families.Binomial())
fit.summary_frame()
```

# SAS to Python mapping

| SAS | Python |
|---|---|
| `DATA`/`SET` of `.sas7bdat`/`.ssp` | `load_meps()` / `pandas.read_sas` / `pyreadstat` |
| `PROC SORT` + `PROC MEANS` / BY-group retain | `df.groupby(...).agg(...)` |
| `PROC FREQ` | `pandas.crosstab`, `meps_utils.crosstab_list` (`/ LIST MISSING`) |
| `PROC FORMAT` | dicts / range lists via `.map()` or `meps_utils.apply_format` |
| `PROC TRANSPOSE` | `pandas.melt` / `pivot` |
| `PROC SURVEYMEANS` (`DOMAIN`, `RATIO`, `QUANTILE`) | `svy_mean`, `svy_total`, `svy_ratio`, `svy_quantile` (samplics) |
| `PROC SURVEYFREQ` | `svy_freq` (samplics) |
| `PROC SURVEYREG` / `PROC SURVEYLOGISTIC` | `svy_glm` (statsmodels) |
| SUDAAN `PROC CROSSTAB` (`SUBPOPN`) | `svy_freq` with a domain |

# Running the examples

``` bash
export MEPS_DIR=/path/to/MEPS
python Python/summary_tables_examples/care_access_2019.py
python Python/workshop_exercises/exercise_1a/Exercise1.py
```

Notes:
- `older_exercises_1996_to_2006/Linking_examples/L2` needs NHIS person files and
  the NHIS-MEPS link file, which are not MEPS PUFs and must be supplied manually.
- `Misc_examples/M10` uses the 2002 inpatient stays file (HC-067D) as well as HC-077D.
- `Estimation_examples/E2` uses the current HC-036 pooled design variables
  (e.g. `STRA9618`/`PSU9618`), which supersede the `STRA9602`/`PSU9602` used in 2007.
- Some programs in the SAS track used SUDAAN; these ports use samplics.
