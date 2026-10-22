"""Shared helpers for analyzing MEPS data in Python.

This module mirrors the building blocks used throughout the SAS/ and R/
tracks of this repository:

* Loading MEPS public use files (``.sas7bdat`` for 2017+, SAS transport
  ``.ssp`` files for 1996-2016) -- the Python equivalent of
  ``DATA ... SET`` / ``PROC XCOPY`` in SAS and ``read_sas`` / ``read.xport``
  in R.
* Downloading MEPS files directly from the MEPS website (``requests`` +
  ``zipfile``), equivalent to ``PROC HTTP`` / ``download.file``.
* Design-based (Taylor-series linearization) survey estimation that preserves
  the MEPS complex sample design -- strata (``VARSTR``), clusters/PSUs
  (``VARPSU``) and weights (``PERWTyyF``, ``SAQWTyyF``, ``DIABWyyF``, ...):

  - :func:`svy_mean`, :func:`svy_total`, :func:`svy_ratio` and
    :func:`svy_freq` use ``samplics.TaylorEstimator`` (``PROC SURVEYMEANS``,
    ``PROC SURVEYFREQ``, R ``svymean`` / ``svytotal`` / ``svyratio``).
  - :func:`svy_quantile` provides survey quantiles (``PROC SURVEYMEANS
    MEDIAN`` / ``QUANTILE``, R ``svyquantile``).
  - :func:`svy_glm` fits survey-weighted GLMs with ``statsmodels`` and
    computes design-based (linearization) standard errors
    (``PROC SURVEYLOGISTIC`` / ``PROC SURVEYREG``, R ``svyglm``).

All estimators use **domain estimation** (like the SAS ``DOMAIN`` statement
or R ``subset(design, ...)``): every record stays in the variance
calculation and records outside the domain simply contribute zero. Never
subset the data frame before estimating -- dropping records (and therefore
whole PSUs) biases the standard errors.
"""

from __future__ import annotations

import io
import math
import os
import warnings
import zipfile
from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
import pandas as pd
from scipy import stats

warnings.filterwarnings("ignore", message="samplics is archived", category=FutureWarning)
from samplics import PopParam, TaylorEstimator  # noqa: E402
import samplics.estimation.expansion as _samplics_expansion  # noqa: E402


class _SafeMath:
    """samplics computes the coefficient of variation as
    ``math.sqrt(var) / estimate`` and raises ZeroDivisionError for domains
    whose estimate is 0 (e.g. a proportion of 0). Returning a numpy float
    from ``sqrt`` makes that division yield ``inf`` instead; the estimates
    and standard errors are unaffected."""

    def __getattr__(self, name):
        return getattr(math, name)

    @staticmethod
    def sqrt(x):
        return np.float64(math.sqrt(x))


_samplics_expansion.math = _SafeMath()
warnings.filterwarnings("ignore", message="divide by zero", category=RuntimeWarning)

__all__ = [
    "MEPS_DIR",
    "meps_file",
    "read_meps",
    "download_meps",
    "load_meps",
    "add_domain",
    "svy_mean",
    "svy_total",
    "svy_ratio",
    "svy_freq",
    "svy_quantile",
    "svy_glm",
    "apply_format",
]

# Folder where MEPS files are stored. The SAS and R examples assume
# C:/MEPS; override with the MEPS_DIR environment variable.
MEPS_DIR = os.environ.get("MEPS_DIR", "C:/MEPS")

MEPS_URL = "https://meps.ahrq.gov/mepsweb/data_files/pufs"

_EXCLUDED = "__not_in_domain__"
_MISSING = "Missing"


# -----------------------------------------------------------------------------
# Loading data
# -----------------------------------------------------------------------------


def meps_file(name: str, meps_dir: str | os.PathLike | None = None) -> Path:
    """Return the path of a MEPS file in ``meps_dir`` (default ``MEPS_DIR``).

    ``name`` may include an extension (``"h216.sas7bdat"``) or be a bare MEPS
    file name (``"h216"``), in which case the ``.sas7bdat`` file is preferred
    and the SAS transport ``.ssp`` file is used as a fallback.
    """
    folder = Path(meps_dir or MEPS_DIR)
    path = folder / name
    if path.suffix:
        return path
    for ext in (".sas7bdat", ".ssp"):
        candidate = folder / f"{name}{ext}"
        if candidate.exists():
            return candidate
    return folder / f"{name}.sas7bdat"


def read_meps(path: str | os.PathLike, columns: Sequence[str] | None = None) -> pd.DataFrame:
    """Read a MEPS public use file into a pandas DataFrame.

    * ``.sas7bdat`` (SAS V9, 2017 and later): read with ``pyreadstat`` (fast,
      supports reading a subset of ``columns``). ``pandas.read_sas(path)``
      gives the same result.
    * ``.ssp`` (SAS transport/XPORT, 1996-2016): read with
      ``pandas.read_sas(path, format="xport")``.

    Column names are upper-cased to match MEPS documentation and character
    columns are returned as ``str``.
    """
    path = Path(path)
    if not path.exists():
        # Some files are published only in one format; accept either one.
        for alt in (".sas7bdat", ".ssp"):
            if path.with_suffix(alt).exists():
                path = path.with_suffix(alt)
                break
    ext = path.suffix.lower()
    if ext == ".sas7bdat":
        try:
            import pyreadstat

            usecols = None
            if columns is not None:
                _, meta = pyreadstat.read_sas7bdat(str(path), metadataonly=True)
                lookup = {c.upper(): c for c in meta.column_names}
                usecols = [lookup[c.upper()] for c in columns if c.upper() in lookup]
            df, _ = pyreadstat.read_sas7bdat(str(path), usecols=usecols)
        except ImportError:  # pragma: no cover - pyreadstat is in requirements.txt
            df = pd.read_sas(path, format="sas7bdat", encoding="latin-1")
    elif ext in (".ssp", ".xpt", ".stc"):
        df = pd.read_sas(path, format="xport", encoding="latin-1")
        # pandas decodes IBM-float zeros with a non-zero exponent byte as
        # ~5.4e-79 instead of 0; snap them back to exact zeros.
        num = df.select_dtypes("number").columns
        df[num] = df[num].mask(df[num].abs() < 1e-70, 0.0)
    else:
        raise ValueError(f"Unsupported MEPS file type: {path}")

    df.columns = [c.upper() for c in df.columns]
    if columns is not None:
        df = df[[c.upper() for c in columns if c.upper() in df.columns]]
    for col in df.columns:
        if df[col].dtype == object or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].astype(object).where(df[col].notna(), "").astype(str).str.strip()
    return df


def download_meps(
    name: str, meps_dir: str | os.PathLike | None = None, overwrite: bool = False
) -> Path:
    """Download and unzip a MEPS public use file from the MEPS website.

    Tries the SAS V9 zip (``.../pufs/<name>/<name>v9.zip``, data years 2017+)
    and then the SAS transport zip (``.../pufs/<name>ssp.zip``, 1996-2016).
    Returns the path of the extracted ``.sas7bdat`` or ``.ssp`` file.
    """
    import requests

    folder = Path(meps_dir or MEPS_DIR)
    folder.mkdir(parents=True, exist_ok=True)
    for ext in (".sas7bdat", ".ssp"):
        existing = folder / f"{name}{ext}"
        if existing.exists() and not overwrite:
            return existing

    urls = [f"{MEPS_URL}/{name}/{name}v9.zip", f"{MEPS_URL}/{name}ssp.zip"]
    for url in urls:
        resp = requests.get(url, timeout=600)
        if resp.status_code != 200 or not resp.content.startswith(b"PK"):
            continue
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            member = next(
                m for m in zf.namelist() if m.lower().endswith((".sas7bdat", ".ssp"))
            )
            target = folder / f"{name}{Path(member).suffix.lower()}"
            with zf.open(member) as src, open(target, "wb") as dst:
                dst.write(src.read())
        return target
    raise FileNotFoundError(f"Could not download MEPS file '{name}' from {urls}")


def load_meps(
    name: str, columns: Sequence[str] | None = None, meps_dir: str | os.PathLike | None = None
) -> pd.DataFrame:
    """Load MEPS file ``name`` from ``meps_dir``, downloading it if needed."""
    path = meps_file(name, meps_dir)
    if not path.exists():
        path = download_meps(name, meps_dir)
    return read_meps(path, columns=columns)


def apply_format(series: pd.Series, fmt, default=None, missing=None) -> pd.Series:
    """Apply a SAS-style ``PROC FORMAT`` value format to a Series.

    ``fmt`` is one of:

    * a dict mapping values to labels (``1 = 'Yes'``),
    * a list of ``(low, high, label)`` tuples for range formats
      (``18-44 = '18-44'``; use ``-np.inf`` / ``np.inf`` for ``LOW`` /
      ``HIGH``; ranges are inclusive and the first match wins),
    * a callable.

    Missing values (SAS ``.``) become ``missing`` when given. Unmapped values
    become ``default`` (or keep their original value when ``default`` is
    None), mirroring how SAS prints unformatted values (``OTHER =``).
    """
    if callable(fmt):
        return series.map(fmt)
    if isinstance(fmt, list):
        vals = pd.to_numeric(series, errors="coerce")
        mapped = pd.Series(np.nan, index=series.index, dtype=object)
        for low, high, label in reversed(fmt):
            mapped = mapped.mask((vals >= low) & (vals <= high), label)
    else:
        mapped = series.map(fmt)
    if missing is not None:
        mapped = mapped.mask(series.isna(), missing)
    fallback = series if default is None else default
    return mapped.where(mapped.notna(), fallback)


# -----------------------------------------------------------------------------
# Survey estimation helpers (samplics)
# -----------------------------------------------------------------------------


def _as_list(x) -> list:
    if x is None:
        return []
    if isinstance(x, (str, bytes)):
        return [x]
    return list(x)


def add_domain(df: pd.DataFrame, condition, weight: str, name: str = "domain") -> pd.DataFrame:
    """Add a 0/1 domain flag and set zero weights to 1 outside the domain.

    Mirrors the SAS pattern used throughout the examples, e.g.::

        domain = (ACCELI42 = 1);
        if domain = 0 and PERWT19F = 0 then PERWT19F = 1;

    so that observations outside the domain are not dropped by the survey
    procedures (they are given positive weight but contribute zero to every
    domain estimate).
    """
    out = df.copy()
    out[name] = np.asarray(condition, dtype=bool).astype(int)
    out.loc[(out[name] == 0) & (out[weight] == 0), weight] = 1
    return out


def _domain_key(df: pd.DataFrame, domain: list[str], missing: bool = True) -> pd.Series:
    """Combine one or more domain columns into a single string key."""
    if not domain:
        return pd.Series("All", index=df.index)
    parts = []
    for col in domain:
        s = df[col]
        if missing:
            s = s.astype(object).where(s.notna(), _MISSING)
        parts.append(s.astype(str))
    key = parts[0]
    for p in parts[1:]:
        key = key + "\x1f" + p
    if not missing:
        any_na = df[domain].isna().any(axis=1)
        key = key.where(~any_na, _EXCLUDED)
    return key


def _split_key(out: pd.DataFrame, df: pd.DataFrame, domain: list[str]) -> pd.DataFrame:
    """Split the combined domain key back into the original domain columns."""
    if not domain:
        return out.drop(columns="_key")
    parts = out["_key"].str.split("\x1f", expand=True)
    parts.columns = domain
    for col in domain:
        if pd.api.types.is_numeric_dtype(df[col]):
            parts[col] = parts[col].map(lambda v: v if v == _MISSING else _to_number(v))
    out = pd.concat([parts.reset_index(drop=True), out.drop(columns="_key").reset_index(drop=True)], axis=1)
    return out


def _to_number(v: str):
    try:
        f = float(v)
    except ValueError:
        return v
    return int(f) if f.is_integer() else f


def _design(df: pd.DataFrame, weight: str, strata: str | None, psu: str | None):
    w = df[weight].astype(float).to_numpy()
    st = df[strata].to_numpy() if strata else None
    cl = df[psu].to_numpy() if psu else None
    if st is not None and cl is not None:
        # Nest PSUs within strata (R: svydesign(..., nest = TRUE)).
        cl = pd.Series(st).astype(str).str.cat(pd.Series(cl).astype(str), sep="_").to_numpy()
    return w, st, cl


def _taylor(param, y, w, st, cl, key, x=None, as_factor=False):
    est = TaylorEstimator(param)
    est.estimate(
        y=y,
        samp_weight=w,
        x=x,
        stratum=st,
        psu=cl,
        domain=key,
        as_factor=as_factor,
    )
    return est


def _linear_estimate(
    param,
    df: pd.DataFrame,
    variables,
    weight: str,
    domain=None,
    strata: str | None = "VARSTR",
    psu: str | None = "VARPSU",
    missing: bool = True,
) -> pd.DataFrame:
    domain = _as_list(domain)
    w, st, cl = _design(df, weight, strata, psu)
    base_key = _domain_key(df, domain, missing=missing)
    rows = []
    for var in _as_list(variables):
        y = df[var].astype(float)
        # Records with a missing analysis variable are treated as out of
        # domain (R: na.rm = TRUE on a subset design) rather than dropped.
        key = base_key.where(y.notna(), _EXCLUDED)
        est = _taylor(param, y.fillna(0).to_numpy(), w, st, cl, key.to_numpy())
        res = _collect(est, var)
        rows.append(res)
    out = pd.concat(rows, ignore_index=True)
    out = out[out["_key"] != _EXCLUDED]
    out = _split_key(out, df, domain)
    n = _domain_counts(df, domain, base_key, variables)
    out = out.merge(n, on=domain + ["variable"], how="left") if domain else out.merge(
        n, on=["variable"], how="left"
    )
    return out


def _collect(est, var) -> pd.DataFrame:
    point, se = est.point_est, est.stderror
    if not isinstance(point, dict):
        point, se = {"All": point}, {"All": se}
    return pd.DataFrame(
        {
            "_key": [str(k) for k in point.keys()],
            "variable": var,
            "estimate": [float(v) for v in point.values()],
            "se": [float(se[k]) for k in point.keys()],
        }
    )


def _domain_counts(df, domain, base_key, variables) -> pd.DataFrame:
    frames = []
    for var in _as_list(variables):
        valid = df[var].notna()
        counts = base_key[valid].value_counts().rename("n").reset_index()
        counts.columns = ["_key", "n"]
        counts = counts[counts["_key"] != _EXCLUDED]
        counts["variable"] = var
        frames.append(counts)
    counts = pd.concat(frames, ignore_index=True)
    return _split_key(counts, df, domain)


def svy_mean(df, variables, weight, domain=None, strata="VARSTR", psu="VARPSU", missing=True):
    """Survey means (``PROC SURVEYMEANS MEAN`` / R ``svymean``).

    Parameters
    ----------
    df : DataFrame containing the analysis, design and domain variables.
    variables : column name or list of column names to estimate.
    weight : weight variable, e.g. ``"PERWT19F"``.
    domain : optional column name or list of columns defining domains
        (the SAS ``DOMAIN a*b`` statement). Every record remains in the
        variance calculation.
    missing : treat missing domain values as their own category (the SAS
        ``MISSING`` option). If False, records with a missing domain value
        are excluded from every domain.

    Returns a DataFrame with the domain columns, ``variable``, ``estimate``,
    ``se`` (standard error) and ``n`` (unweighted number of observations).
    """
    return _linear_estimate(PopParam.mean, df, variables, weight, domain, strata, psu, missing)


def svy_total(df, variables, weight, domain=None, strata="VARSTR", psu="VARPSU", missing=True):
    """Survey totals (``PROC SURVEYMEANS SUM`` / R ``svytotal``).

    ``se`` corresponds to the SAS ``StdDev`` of the sum. See :func:`svy_mean`
    for parameter descriptions.
    """
    return _linear_estimate(PopParam.total, df, variables, weight, domain, strata, psu, missing)


def svy_ratio(
    df, numerator, denominator, weight, domain=None, strata="VARSTR", psu="VARPSU", missing=True
):
    """Survey ratio of totals ``sum(w*num) / sum(w*den)``.

    Equivalent to the SAS ``RATIO num / den`` statement in ``PROC
    SURVEYMEANS`` and to R ``svyratio``.
    """
    domain = _as_list(domain)
    w, st, cl = _design(df, weight, strata, psu)
    base_key = _domain_key(df, domain, missing=missing)
    rows = []
    for num in _as_list(numerator):
        for den in _as_list(denominator):
            y, x = df[num].astype(float), df[den].astype(float)
            valid = y.notna() & x.notna()
            key = base_key.where(valid, _EXCLUDED)
            est = _taylor(
                PopParam.ratio,
                y.fillna(0).to_numpy(),
                w,
                st,
                cl,
                key.to_numpy(),
                x=x.fillna(0).to_numpy(),
            )
            res = _collect(est, f"{num} / {den}")
            res.insert(1, "numerator", num)
            res.insert(2, "denominator", den)
            rows.append(res)
    out = pd.concat(rows, ignore_index=True)
    out = out[out["_key"] != _EXCLUDED]
    return _split_key(out, df, domain)


def svy_freq(
    df,
    table,
    weight,
    domain=None,
    strata="VARSTR",
    psu="VARPSU",
    missing=True,
):
    """Survey frequency table (``PROC SURVEYFREQ`` / R ``svytotal`` +
    ``svymean`` on a factor).

    ``table`` is the variable whose levels are tabulated (the last variable
    in a SAS ``TABLES a*b*c`` request); ``domain`` lists the preceding
    variables (the "row" variables). The result contains, for each domain and
    level:

    * ``n`` -- unweighted frequency (SAS ``Frequency``)
    * ``wgt_freq`` / ``wgt_freq_se`` -- weighted frequency and its standard
      error (SAS ``WgtFreq`` / ``StdDev``)
    * ``percent`` / ``percent_se`` -- percent within the domain (SAS
      ``RowPercent`` / ``RowStdErr`` when ``domain`` is given, otherwise
      ``Percent`` / ``StdErr``)

    With ``missing=True`` (the SAS ``MISSING`` option), missing values are a
    separate level.
    """
    domain = _as_list(domain)
    w, st, cl = _design(df, weight, strata, psu)
    key = _domain_key(df, domain, missing=missing)
    y = df[table]
    if missing:
        y = y.astype(object).where(y.notna(), _MISSING)
    else:
        key = key.where(y.notna(), _EXCLUDED)
    y_str = y.astype(str)
    levels = sorted(y_str[key != _EXCLUDED].unique(), key=_sort_key)

    frames = []
    for level in levels:
        ind = (y_str == level).astype(float).to_numpy()
        tot = _taylor(PopParam.total, ind, w, st, cl, key.to_numpy())
        pct = _taylor(PopParam.mean, ind, w, st, cl, key.to_numpy())
        t = _collect(tot, table).rename(columns={"estimate": "wgt_freq", "se": "wgt_freq_se"})
        p = _collect(pct, table).rename(columns={"estimate": "percent", "se": "percent_se"})
        res = t.merge(p[["_key", "percent", "percent_se"]], on="_key")
        res["level"] = level
        frames.append(res)
    out = pd.concat(frames, ignore_index=True)
    out = out[out["_key"] != _EXCLUDED].copy()
    out["percent"] *= 100
    out["percent_se"] *= 100
    counts = (
        pd.DataFrame({"_key": key, "level": y_str})
        .value_counts()
        .rename("n")
        .reset_index()
    )
    out = out.merge(counts, on=["_key", "level"], how="left")
    out["n"] = out["n"].fillna(0).astype(int)
    out = out.rename(columns={"level": table}).drop(columns="variable")
    out[table] = out[table].map(lambda v: v if v == _MISSING else _to_number(v))
    out = _split_key(out, df, domain)
    cols = domain + [table, "n", "wgt_freq", "wgt_freq_se", "percent", "percent_se"]
    return out[cols]


def _sort_key(v: str):
    num = _to_number(v)
    return (0, num, "") if isinstance(num, (int, float)) else (1, 0, str(v))


def _weighted_quantile(y: np.ndarray, w: np.ndarray, q: float) -> float:
    order = np.argsort(y, kind="mergesort")
    y, w = y[order], w[order]
    cw = np.cumsum(w) / w.sum()
    idx = np.searchsorted(cw, q - 1e-12, side="left")
    return float(y[min(idx, len(y) - 1)])


def svy_quantile(
    df,
    variables,
    weight,
    quantiles=(0.5,),
    domain=None,
    strata="VARSTR",
    psu="VARPSU",
    missing=True,
    alpha=0.05,
):
    """Survey quantiles (``PROC SURVEYMEANS MEDIAN`` / R ``svyquantile``).

    The point estimate is the weighted empirical quantile; the standard error
    uses the Woodruff method: a design-based confidence interval for the
    proportion of the population at or below the quantile (estimated with
    ``samplics``) is inverted on the quantile scale, and
    ``se = (upper - lower) / (2 * z)``.

    Note: SAS, R, and Stata use slightly different quantile definitions, so
    medians may differ slightly between software.
    """
    domain = _as_list(domain)
    w, st, cl = _design(df, weight, strata, psu)
    base_key = _domain_key(df, domain, missing=missing)
    z = stats.norm.ppf(1 - alpha / 2)
    rows = []
    for var in _as_list(variables):
        y_all = df[var].astype(float)
        key = base_key.where(y_all.notna(), _EXCLUDED)
        for k in pd.unique(key):
            if k == _EXCLUDED:
                continue
            mask = (key == k).to_numpy()
            y_d, w_d = y_all.to_numpy()[mask], w[mask]
            for q in _as_list(quantiles):
                qhat = _weighted_quantile(y_d, w_d, q)
                ind = np.where(mask, (y_all.to_numpy() <= qhat).astype(float), 0.0)
                dom = np.where(mask, "in", "out")
                est = _taylor(PopParam.mean, ind, w, st, cl, dom)
                p_se = float(est.stderror["in"])
                lo = _weighted_quantile(y_d, w_d, max(q - z * p_se, 0.0))
                hi = _weighted_quantile(y_d, w_d, min(q + z * p_se, 1.0))
                rows.append(
                    {
                        "_key": k,
                        "variable": var,
                        "quantile": q,
                        "estimate": qhat,
                        "se": (hi - lo) / (2 * z),
                        "lower_ci": lo,
                        "upper_ci": hi,
                    }
                )
    out = pd.DataFrame(rows)
    return _split_key(out, df, domain)


# -----------------------------------------------------------------------------
# Survey regression (statsmodels)
# -----------------------------------------------------------------------------


class SurveyGLMResult:
    """Result of :func:`svy_glm` with design-based standard errors."""

    def __init__(self, fit, cov, df_resid, n, family_name):
        self.fit = fit
        self.params = fit.params
        self.cov_params = pd.DataFrame(cov, index=fit.params.index, columns=fit.params.index)
        self.bse = pd.Series(np.sqrt(np.diag(cov)), index=fit.params.index)
        self.df_resid = df_resid
        self.nobs = n
        self.family_name = family_name
        self.tvalues = self.params / self.bse
        self.pvalues = pd.Series(
            2 * stats.t.sf(np.abs(self.tvalues), df_resid), index=fit.params.index
        )

    def summary_frame(self, alpha: float = 0.05) -> pd.DataFrame:
        """Coefficient table (estimate, SE, t, p-value, Wald CI).

        For logistic models, odds ratios and their confidence intervals are
        added (SAS SURVEYLOGISTIC "Odds Ratio Estimates").
        """
        z = stats.t.ppf(1 - alpha / 2, self.df_resid)
        out = pd.DataFrame(
            {
                "estimate": self.params,
                "se": self.bse,
                "t": self.tvalues,
                "p_value": self.pvalues,
                "lower_ci": self.params - z * self.bse,
                "upper_ci": self.params + z * self.bse,
            }
        )
        if self.family_name == "Binomial":
            out["odds_ratio"] = np.exp(out["estimate"])
            out["or_lower_ci"] = np.exp(out["lower_ci"])
            out["or_upper_ci"] = np.exp(out["upper_ci"])
        return out

    def __repr__(self) -> str:
        with pd.option_context("display.width", 200, "display.max_columns", 20):
            return (
                f"Survey-weighted GLM ({self.family_name}), n = {self.nobs}, "
                f"design df = {self.df_resid}\n{self.summary_frame()}"
            )


def svy_glm(
    formula: str,
    df: pd.DataFrame,
    weight: str,
    family=None,
    domain=None,
    strata: str | None = "VARSTR",
    psu: str | None = "VARPSU",
) -> SurveyGLMResult:
    """Survey-weighted generalized linear model with linearization SEs.

    Equivalent to SAS ``PROC SURVEYLOGISTIC`` (``family=sm.families.
    Binomial()``) and ``PROC SURVEYREG`` (default Gaussian family), and to R
    ``svyglm``.

    The model is fit with ``statsmodels.formula.api.glm`` using the survey
    weights; the variance of the coefficients is the design-based sandwich
    estimator ``A^-1 B A^-1`` where ``B`` is the stratified, with-replacement
    between-PSU variance of the weighted score contributions. ``domain`` is a
    boolean column/array (e.g. ``"domain"`` for SAS ``DOMAIN domain('1')``):
    the model is fit to records in the domain but all PSUs remain in the
    variance calculation.
    """
    import statsmodels.api as sm
    import statsmodels.formula.api as smf

    family = family or sm.families.Gaussian()
    if domain is None:
        in_domain = np.ones(len(df), dtype=bool)
    elif isinstance(domain, str):
        in_domain = df[domain].astype(bool).to_numpy()
    else:
        in_domain = np.asarray(domain, dtype=bool)

    data = df.loc[in_domain & (df[weight] > 0)].copy()
    # Rescale weights to mean 1 for numerically stable IRLS; the coefficients
    # and the sandwich variance are invariant to this scaling.
    data["_w"] = data[weight].astype(float) / data[weight].astype(float).mean()
    model = smf.glm(formula, data=data, family=family, var_weights=data["_w"])
    fit = model.fit()
    used = model.data.row_labels  # rows remaining after dropping missing values

    X = model.exog
    y = model.endog
    w = data.loc[used, "_w"].to_numpy()
    mu = fit.fittedvalues.to_numpy()
    link = family.link
    dmu_deta = 1.0 / link.deriv(mu)
    resid = (y - mu) / family.variance(mu) * dmu_deta
    scores = X * (w * resid)[:, None]
    bread = fit.normalized_cov_params
    infl = scores @ np.asarray(bread)

    # Place influence values back onto the full design (zeros outside domain).
    full = pd.DataFrame(0.0, index=df.index, columns=range(X.shape[1]))
    full.loc[used, :] = infl
    st = df[strata].astype(str) if strata else pd.Series("1", index=df.index)
    cl = (st + "_" + df[psu].astype(str)) if psu else pd.Series(np.arange(len(df)), index=df.index).astype(str)
    cov = _stratified_cluster_cov(full.to_numpy(), st.to_numpy(), cl.to_numpy())

    n_psu = pd.Series(cl).nunique()
    n_strata = pd.Series(st).nunique()
    df_resid = n_psu - n_strata - X.shape[1] + 1
    return SurveyGLMResult(fit, cov, df_resid, len(used), type(family).__name__)


def _stratified_cluster_cov(z: np.ndarray, strata: np.ndarray, clusters: np.ndarray) -> np.ndarray:
    """With-replacement Taylor variance of column totals of ``z``.

    ``V = sum_h n_h/(n_h - 1) * sum_i (z_hi - zbar_h)(z_hi - zbar_h)'`` where
    ``z_hi`` are PSU totals. Strata with a single PSU are centered at the
    grand mean of PSU totals (R ``options(survey.lonely.psu = "adjust")``).
    """
    psu_tot = pd.DataFrame(z).groupby([strata, clusters]).sum()
    strata_idx = psu_tot.index.get_level_values(0)
    grand_mean = psu_tot.to_numpy().mean(axis=0)
    p = z.shape[1]
    cov = np.zeros((p, p))
    for h, block in psu_tot.groupby(strata_idx):
        vals = block.to_numpy()
        n_h = vals.shape[0]
        if n_h > 1:
            dev = vals - vals.mean(axis=0)
            cov += n_h / (n_h - 1) * dev.T @ dev
        else:
            dev = vals - grand_mean
            cov += dev.T @ dev
    return cov


def crosstab_list(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    """Unweighted ``PROC FREQ ... / LIST`` style table of value combinations."""
    return (
        df[list(columns)]
        .astype(object)
        .fillna(".")
        .value_counts(sort=False)
        .rename("COUNT")
        .reset_index()
        .sort_values(list(columns))
        .reset_index(drop=True)
    )


__all__.append("crosstab_list")
