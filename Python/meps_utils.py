"""Shared helpers for the Python track of the MEPS-HC examples.

Each helper mirrors a SAS construct used throughout ``SAS/``:

=====================================================  ==========================================
SAS                                                    Python (this module / pandas)
=====================================================  ==========================================
``DATA x; SET "C:/MEPS/h216.sas7bdat";``               ``read_meps("h216")`` -> ``pandas.read_sas``
``PROC XCOPY in=... out=WORK IMPORT`` (``.ssp``)       ``read_meps("h192")`` -> ``pandas.read_sas(format="xport")``
``PROC HTTP`` + ``FILENAME ZIP``                       ``download_meps("h216")``
``PROC FORMAT; VALUE ...``                             ``sas_format(series, {...})``
``PROC FREQ ... / LIST MISSING``                       ``freq_list(df, cols)``
``PROC SURVEYMEANS`` (SUM/MEAN/DOMAIN)                 ``surveymeans(...)``  (samplics TaylorEstimator)
``PROC SURVEYMEANS ... RATIO``                         ``surveyratio(...)``  (samplics TaylorEstimator)
``PROC SURVEYMEANS ... MEDIAN/QUANTILE``               ``surveyquantile(...)`` (Woodruff, samplics variance)
``PROC SURVEYFREQ ... TABLES a*b / ROW``               ``surveyfreq(...)``   (samplics TaylorEstimator)
``PROC SURVEYLOGISTIC`` / ``PROC SURVEYREG``           ``surveyreg(...)``    (samplics SurveyGLM)
=====================================================  ==========================================

Survey design
-------------
Every estimator takes the MEPS design variables -- ``strata`` (VARSTR), ``cluster``
(VARPSU) and ``weight`` (PERWTyyF, SAQWTyyF, DIABWyyF, ...) -- and uses
Taylor-series linearization with PSUs nested within strata, exactly like the SAS
SURVEY procedures. Subpopulations are analyzed with ``domain=`` (SAS ``DOMAIN``):
all records stay in the variance calculation and records outside the domain
contribute zero. Do not filter rows before estimating.

As in SAS, observations with a missing or non-positive weight are excluded, and
strata with a single PSU contribute no variance.
"""

from __future__ import annotations

import io
import math
import os
import re
import warnings
import zipfile
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import numpy as np
import pandas as pd
from scipy import stats

warnings.filterwarnings("ignore", category=FutureWarning, module="samplics")
warnings.filterwarnings("ignore", message=".*samplics.*", category=FutureWarning)
import samplics.estimation.expansion as _expansion  # noqa: E402
from samplics import PopParam, TaylorEstimator  # noqa: E402
from samplics.regression import SurveyGLM  # noqa: E402
from samplics.utils.types import ModelType, SinglePSUEst  # noqa: E402


class _Math:
    """samplics computes CV = math.sqrt(var) / estimate, which raises
    ZeroDivisionError when a domain estimate is exactly 0 (e.g. a percent of 0).
    Returning a numpy float yields inf instead. Estimates and SEs are unchanged."""

    def __getattr__(self, name):
        return getattr(math, name)

    @staticmethod
    def sqrt(x):
        return np.float64(math.sqrt(x))


_expansion.math = _Math()

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 30)
pd.set_option("display.max_rows", 500)

# Local folder holding MEPS files. The SAS code hardcodes C:/MEPS; override it
# with the MEPS_DIR environment variable.
MEPS_DIR = Path(os.environ.get("MEPS_DIR", "C:/MEPS"))
MEPS_URL = "https://meps.ahrq.gov/mepsweb/data_files/pufs"

__all__ = [
    "MEPS_DIR",
    "read_meps",
    "download_meps",
    "sas_format",
    "freq_list",
    "surveymeans",
    "design_df",
    "clm",
    "surveyratio",
    "surveyquantile",
    "surveyfreq",
    "surveyreg",
    "show",
]


# -----------------------------------------------------------------------------
# Loading data (DATA/SET, PROC XCOPY, PROC HTTP)
# -----------------------------------------------------------------------------


def _find(name: str, folder: Path) -> Path | None:
    stem = Path(name).stem if Path(name).suffix.lower() in (".sas7bdat", ".ssp", ".dta") else name
    if not folder.exists():
        return None
    files = {p.name.lower(): p for p in folder.iterdir()}
    for ext in (".sas7bdat", ".ssp", ".dta"):
        if f"{stem.lower()}{ext}" in files:
            return files[f"{stem.lower()}{ext}"]
    return None


def download_meps(name: str, meps_dir: str | os.PathLike | None = None) -> Path:
    """Download and unzip a MEPS public use file into ``meps_dir``.

    Tries the SAS V9 zip (``pufs/<name>/<name>v9.zip``, 2017+), then the SAS
    transport zip (``pufs/<name>ssp.zip``, 1996-2016), then the Stata zip.
    Link files such as ``h206if1`` live in the folder of their base name
    (``h206i``)."""
    import requests

    folder = Path(meps_dir or MEPS_DIR)
    folder.mkdir(parents=True, exist_ok=True)
    found = _find(name, folder)
    if found:
        return found
    n = name.lower()
    m = re.fullmatch(r"(h\d+[a-z]*?)(f\d+)?", n)
    base = m.group(1) if m else n
    folders = [n, base] + (["h036"] if n.startswith("h36") else [])
    urls = [f"{MEPS_URL}/{f}/{n}v9.zip" for f in dict.fromkeys(folders)]
    urls += [f"{MEPS_URL}/{n}ssp.zip"] + [f"{MEPS_URL}/{f}/{n}ssp.zip" for f in dict.fromkeys(folders)]
    urls += [f"{MEPS_URL}/{f}/{n}dta.zip" for f in dict.fromkeys(folders)]
    for url in urls:
        r = requests.get(url, timeout=600)
        if r.status_code != 200 or r.content[:2] != b"PK":
            continue
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            members = [i for i in z.namelist() if i.lower().endswith((".sas7bdat", ".ssp", ".dta"))]
            if not members:  # e.g. a SAS CPORT archive, which Python cannot read
                continue
            for member in members:
                target = folder / Path(member).name.lower()
                target.write_bytes(z.read(member))
        found = _find(name, folder)
        if found:
            return found
    raise FileNotFoundError(f"Could not download MEPS file {name!r} from {MEPS_URL}")


def read_meps(
    name: str,
    columns: Sequence[str] | None = None,
    meps_dir: str | os.PathLike | None = None,
    download: bool = True,
) -> pd.DataFrame:
    """Load a MEPS file from ``meps_dir`` (default ``MEPS_DIR``).

    ``name`` is the MEPS file name (``"h216"``, ``"h192"``, ``"h206if1"``).
    * ``.sas7bdat`` (2017+): ``pandas.read_sas(path)``      (SAS: DATA; SET)
    * ``.ssp`` (1996-2016):  ``pandas.read_sas(path, format="xport")``  (SAS: PROC XCOPY)
    Column names are upper-cased; character columns are stripped strings.
    The file is downloaded from the MEPS website if it is not found locally.
    """
    folder = Path(meps_dir or MEPS_DIR)
    path = _find(name, folder)
    if path is None:
        if not download:
            raise FileNotFoundError(f"{name} not found in {folder}")
        path = download_meps(name, folder)
    ext = path.suffix.lower()
    if ext == ".sas7bdat":
        df = pd.read_sas(path, format="sas7bdat", encoding="latin-1")
    elif ext == ".ssp":
        try:
            df = pd.read_sas(path, format="xport", encoding="latin-1")
        except ValueError:  # a few old XPORT headers are rejected by pandas
            import pyreadstat

            try:
                df, _ = pyreadstat.read_xport(str(path), encoding="latin-1")
            except pyreadstat.ReadstatError:  # e.g. VM/CMS headers with no character set
                df, _ = pyreadstat.read_xport(str(path))
        # IBM floats: pandas decodes some zeros as ~5e-79; snap them to 0.
        num = df.select_dtypes("number").columns
        df[num] = df[num].mask(df[num].abs() < 1e-70, 0.0)
    else:
        df = pd.read_stata(path, convert_categoricals=False)
    df.columns = [c.upper() for c in df.columns]
    if columns is not None:
        df = df[[c.upper() for c in columns]].copy()
    for col in df.columns:
        if df[col].dtype == object or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].astype("string").str.strip().fillna("").astype(object)
    return df


# -----------------------------------------------------------------------------
# PROC FORMAT and PROC FREQ
# -----------------------------------------------------------------------------


def sas_format(values: pd.Series, fmt: Mapping, other=None, missing=None) -> pd.Series:
    """Apply a SAS-style value format.

    ``fmt`` maps a value, a tuple of values, or a ``range`` given as
    ``slice(lo, hi)`` (inclusive; ``None`` = LOW/HIGH) to a label::

        sas_format(df.AGELAST, {slice(None, 4): "Under 5", slice(5, 17): "5-17",
                                slice(18, None): "18+"})

    Unmatched values get ``other`` (default: the value itself, as SAS does);
    missing values get ``missing`` if given.
    """
    out = pd.Series(pd.NA, index=values.index, dtype=object)
    done = pd.Series(False, index=values.index)
    for key, label in fmt.items():
        if isinstance(key, slice):
            m = values.notna()
            if key.start is not None:
                m &= values >= key.start
            if key.stop is not None:
                m &= values <= key.stop
        elif isinstance(key, tuple):
            m = values.isin(key)
        else:
            m = values == key
        m &= ~done
        out[m] = label
        done |= m
    rest = ~done & values.notna()
    out[rest] = values[rest] if other is None else other
    if missing is not None:
        out[values.isna()] = missing
    return out


def freq_list(df: pd.DataFrame, cols: Sequence[str]) -> pd.DataFrame:
    """PROC FREQ ... TABLES a*b*c / LIST MISSING."""
    t = df.groupby(list(cols), dropna=False).size().rename("Frequency").reset_index()
    t["Percent"] = 100 * t["Frequency"] / t["Frequency"].sum()
    t["Cumulative Frequency"] = t["Frequency"].cumsum()
    t["Cumulative Percent"] = 100 * t["Cumulative Frequency"] / t["Frequency"].sum()
    return t


def show(title: str, df: pd.DataFrame, floatfmt: str = "{:,.6f}") -> None:
    """Print a titled table (PROC PRINT)."""
    print(f"\n{title}\n" + "-" * len(title))
    with pd.option_context("display.float_format", floatfmt.format):
        print(df.to_string(index=False))


# -----------------------------------------------------------------------------
# Survey estimation (PROC SURVEYMEANS / SURVEYFREQ / SURVEYLOGISTIC / SURVEYREG)
# -----------------------------------------------------------------------------

_OUT = "\x00out"  # domain key for records outside the requested domains


def _design(df: pd.DataFrame, weight: str, strata: str | None, cluster: str | None) -> pd.DataFrame:
    """Records used by the SAS SURVEY procedures (positive, non-missing weight)."""
    w = pd.to_numeric(df[weight], errors="coerce")
    return df.loc[w.notna() & (w > 0)]


def _domain_key(d: pd.DataFrame, domain: Sequence[str] | None, missing: bool) -> pd.Series | None:
    if not domain:
        return None
    parts = d[list(domain)]
    txt = parts.apply(lambda s: s.map(str))  # astype(str) keeps NaN as missing in pandas 3
    key = txt.agg("\x01".join, axis=1) if len(domain) > 1 else txt.iloc[:, 0]
    if not missing:
        key = key.where(parts.notna().all(axis=1), _OUT)
    return key


def _levels(d: pd.DataFrame, domain: Sequence[str], key: pd.Series) -> pd.DataFrame:
    lv = d[list(domain)].assign(_key=key.values).drop_duplicates("_key")
    return lv[lv["_key"] != _OUT].set_index("_key")


def _taylor(param, y, d, weight, strata, cluster, key, x=None):
    est = TaylorEstimator(param)
    kw = dict(
        y=np.asarray(y, dtype=object if param == PopParam.prop else float),
        samp_weight=d[weight].to_numpy(float),
        stratum=d[strata].to_numpy() if strata else None,
        psu=d[cluster].to_numpy() if cluster else None,
        single_psu=SinglePSUEst.skip,
    )
    if x is not None:
        kw["x"] = np.asarray(x, dtype=float)
    if key is not None:
        kw["domain"] = np.asarray(key, dtype=object)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        est.estimate(**kw)
    return est


def _as_list(x) -> list:
    if x is None:
        return []
    return [x] if isinstance(x, str) else list(x)


def _df_design(d, strata, cluster) -> int:
    """Design degrees of freedom: #PSUs - #strata."""
    if not cluster:
        return len(d) - (d[strata].nunique() if strata else 1)
    s = d[strata] if strata else 0
    return d.assign(_s=s).groupby(["_s", cluster]).ngroups - (d[strata].nunique() if strata else 1)


def surveymeans(
    df: pd.DataFrame,
    var: str | Sequence[str],
    weight: str,
    strata: str | None = "VARSTR",
    cluster: str | None = "VARPSU",
    domain: str | Sequence[str] | None = None,
    missing: bool = False,
) -> pd.DataFrame:
    """PROC SURVEYMEANS ... SUM MEAN; [DOMAIN a*b;] VAR ...;

    Returns one row per variable (and domain level) with SAS column names:
    ``N``, ``SumWgt``, ``Mean``, ``StdErr`` (of the mean), ``Sum`` and ``StdDev``
    (standard error of the sum).

    ``domain`` lists the crossed DOMAIN variables. ``missing=True`` is the SAS
    MISSING option (missing domain values form their own level); otherwise
    records with a missing domain value are outside every domain. Records with a
    missing analysis value are dropped (no DOMAIN) or kept in the design but
    placed outside every domain (DOMAIN), matching SAS.
    """
    d = _design(df, weight, strata, cluster)
    domain = _as_list(domain)
    key = _domain_key(d, domain, missing)
    rows = []
    for v in _as_list(var):
        y = pd.to_numeric(d[v], errors="coerce")
        ok = y.notna()
        if key is None:
            dd, yy, k = d.loc[ok], y[ok], None
        else:
            # As in SAS DOMAIN analysis, records with a missing analysis value stay in
            # the design (keeping every stratum/PSU) but fall outside every domain.
            dd, yy, k = d, y.fillna(0.0), key.where(ok, _OUT)
        mean = _taylor(PopParam.mean, yy, dd, weight, strata, cluster, k)
        tot = _taylor(PopParam.total, yy, dd, weight, strata, cluster, k)
        if k is None:
            rows.append(
                dict(VarName=v, N=len(dd), SumWgt=dd[weight].sum(), Mean=mean.point_est,
                     StdErr=mean.stderror, Sum=tot.point_est, StdDev=tot.stderror)
            )
            continue
        levels = _levels(dd, domain, k)
        n = k.value_counts()
        sw = dd[weight].groupby(k.values).sum()
        for lv, vals in levels.iterrows():
            rows.append(
                dict(**vals.to_dict(), VarName=v, N=int(n[lv]), SumWgt=sw[lv],
                     Mean=mean.point_est[lv], StdErr=mean.stderror[lv],
                     Sum=tot.point_est[lv], StdDev=tot.stderror[lv])
            )
    out = pd.DataFrame(rows)
    if domain:
        out = out.sort_values(["VarName"] + domain, key=lambda s: s.map(_sortkey), kind="stable")
        out = out.set_index("VarName").loc[_as_list(var)].reset_index()
        out = out[domain + [c for c in out.columns if c not in domain]]
    return out.reset_index(drop=True)


def design_df(df: pd.DataFrame, strata: str = "VARSTR", cluster: str = "VARPSU", weight: str | None = None) -> int:
    """Design degrees of freedom used by SAS for t-based CLMs: #PSUs - #strata."""
    d = df if weight is None else df[df[weight] > 0]
    return int(d.groupby(strata)[cluster].nunique().sum() - d[strata].nunique())


def clm(t: pd.DataFrame, ddf: int, est: str = "Mean", se: str = "StdErr", alpha: float = 0.05) -> pd.DataFrame:
    """Add SAS CV and (1 - alpha) CLM / CLSUM columns to a survey* result table."""
    q = stats.t.ppf(1 - alpha / 2, ddf)
    t = t.copy()
    t["CV"] = t[se] / t[est]
    t["LowerCL"], t["UpperCL"] = t[est] - q * t[se], t[est] + q * t[se]
    return t


def _sortkey(v):
    return (1, "") if pd.isna(v) else (0, v) if not isinstance(v, str) else (0.5, v)


def surveyratio(
    df: pd.DataFrame,
    numerator: str,
    denominator: str,
    weight: str,
    strata: str | None = "VARSTR",
    cluster: str | None = "VARPSU",
    domain: str | Sequence[str] | None = None,
    missing: bool = False,
) -> pd.DataFrame:
    """PROC SURVEYMEANS; RATIO numerator / denominator; [DOMAIN ...]"""
    d = _design(df, weight, strata, cluster)
    domain = _as_list(domain)
    key = _domain_key(d, domain, missing)
    y = pd.to_numeric(d[numerator], errors="coerce")
    x = pd.to_numeric(d[denominator], errors="coerce")
    ok = y.notna() & x.notna()
    if key is None:
        d, y, x, k = d[ok], y[ok], x[ok], None
    else:
        y, x, k = y.where(ok, 0.0), x.where(ok, 0.0), key.where(ok, _OUT)
    est = _taylor(PopParam.ratio, y, d, weight, strata, cluster, k, x=x)
    if k is None:
        return pd.DataFrame([dict(Numerator=numerator, Denominator=denominator, N=len(d),
                                  Ratio=est.point_est, StdErr=est.stderror)])
    rows = [dict(**vals.to_dict(), Numerator=numerator, Denominator=denominator, N=int((k == lv).sum()),
                 Ratio=est.point_est[lv], StdErr=est.stderror[lv])
            for lv, vals in _levels(d, domain, k).iterrows()]
    return pd.DataFrame(rows).sort_values(domain, key=lambda s: s.map(_sortkey)).reset_index(drop=True)


def surveyquantile(
    df: pd.DataFrame,
    var: str,
    weight: str,
    strata: str | None = "VARSTR",
    cluster: str | None = "VARPSU",
    domain: str | Sequence[str] | None = None,
    q: float = 0.5,
    missing: bool = False,
    alpha: float = 0.05,
) -> pd.DataFrame:
    """PROC SURVEYMEANS MEDIAN / QUANTILE=(q) [DOMAIN ...].

    Point estimate: SAS's interpolated inverse of the weighted empirical CDF.
    Standard error: Woodruff's method -- the design-based SE of the CDF at the
    estimate (a domain proportion, computed with samplics) is turned into a
    confidence interval for ``q`` with t(df = #PSU - #strata) and mapped back
    through the inverse CDF; SE = (upper - lower) / (2 t).
    """
    d = _design(df, weight, strata, cluster)
    y = pd.to_numeric(d[var], errors="coerce")
    d, y = d[y.notna()], y[y.notna()]
    domain = _as_list(domain)
    key = _domain_key(d, domain, missing)
    if key is None:
        key = pd.Series("all", index=d.index)
    t = stats.t.ppf(1 - alpha / 2, _df_design(d, strata, cluster))
    rows = []
    levels = _levels(d, domain, key) if domain else pd.DataFrame(index=["all"])
    w = d[weight].to_numpy(float)
    for lv, vals in levels.iterrows():
        m = (key == lv).to_numpy()
        yv, wv = y.to_numpy(float)[m], w[m]
        xs, inv = np.unique(yv, return_inverse=True)
        cdf = np.cumsum(np.bincount(inv, weights=wv)) / wv.sum()

        def qfun(p):
            p = min(max(p, 0.0), 1.0)
            k = np.searchsorted(cdf, p, side="right") - 1  # F(x_k) <= p < F(x_k+1)
            if k < 0:
                return xs[0]
            if k >= len(xs) - 1:
                return xs[-1]
            return xs[k] + (p - cdf[k]) / (cdf[k + 1] - cdf[k]) * (xs[k + 1] - xs[k])

        est = qfun(q)
        below = (y <= est).astype(float)
        k = key.where(key == lv, _OUT)
        se_p = _taylor(PopParam.mean, below, d, weight, strata, cluster, k).stderror[lv]
        lo, hi = qfun(q - t * se_p), qfun(q + t * se_p)
        rows.append(dict(**vals.to_dict(), VarName=var, Quantile=q, Estimate=est,
                         StdErr=(hi - lo) / (2 * t), LowerCL=lo, UpperCL=hi))
    return pd.DataFrame(rows)


def surveyfreq(
    df: pd.DataFrame,
    tables: Sequence[str],
    weight: str,
    strata: str | None = "VARSTR",
    cluster: str | None = "VARPSU",
    missing: bool = False,
) -> pd.DataFrame:
    """PROC SURVEYFREQ; TABLES a*b[*c] / ROW;

    For each cell returns ``Frequency`` (unweighted), ``WgtFreq`` and ``StdDev``
    (weighted frequency and its SE), ``Percent``/``StdErr`` (of all records),
    and ``RowPercent``/``RowStdErr`` (within the levels of all but the last
    table variable, i.e. ROW percents for two-way tables, ROW percents within
    each layer for three-way tables). ``missing=True`` is the SAS MISSING option.
    """
    tables = list(tables)
    d = _design(df, weight, strata, cluster)
    cell = _domain_key(d, tables, missing)
    if len(tables) == 1:
        rowkey = pd.Series("all", index=d.index).where(cell != _OUT, _OUT)
    else:
        rowkey = _domain_key(d, tables[:-1], missing).where(cell != _OUT, _OUT)
    col = d[tables[-1]].astype(str).where(cell != _OUT, _OUT)
    one = np.ones(len(d))
    tot = _taylor(PopParam.total, one, d, weight, strata, cluster, cell)
    inside = pd.Series(np.where(cell != _OUT, "in", _OUT), index=d.index)
    pct = _taylor(PopParam.prop, cell.where(cell != _OUT, _OUT), d, weight, strata, cluster, inside)
    row = _taylor(PopParam.prop, col, d, weight, strata, cluster, rowkey)
    n = cell.value_counts()
    out = []
    for lv, vals in _levels(d, tables, cell).iterrows():
        rk = rowkey[cell == lv].iloc[0]
        ck = col[cell == lv].iloc[0]
        out.append(dict(**vals.to_dict(), Frequency=int(n[lv]), WgtFreq=tot.point_est[lv],
                        StdDev=tot.stderror[lv], Percent=100 * pct.point_est["in"][lv],
                        StdErr=100 * pct.stderror["in"][lv],
                        RowPercent=100 * row.point_est[rk][ck], RowStdErr=100 * row.stderror[rk][ck]))
    res = pd.DataFrame(out)
    return res.sort_values(tables, key=lambda s: s.map(_sortkey), kind="stable").reset_index(drop=True)


def surveyreg(
    df: pd.DataFrame,
    formula: str,
    weight: str,
    strata: str | None = "VARSTR",
    cluster: str | None = "VARPSU",
    domain: pd.Series | str | None = None,
    family: str = "logistic",
    vadjust: bool = True,
) -> pd.DataFrame:
    """PROC SURVEYLOGISTIC (``family="logistic"``) / PROC SURVEYREG (``"linear"``).

    ``formula`` is a patsy formula, e.g. ``"Y ~ C(SEX, Treatment(2)) + AGE"``
    (``Treatment(ref)`` is the SAS ``CLASS x(REF='...')`` / ``PARAM=REF``).
    Coefficients and their linearized covariance come from samplics
    ``SurveyGLM`` (weights, strata, PSUs). ``domain`` (a boolean column or
    Series) is the SAS ``DOMAIN`` statement: records outside the domain keep
    their PSU in the variance estimate but get zero weight. ``vadjust=True``
    applies the SAS default variance adjustment (n-1)/(n-p).

    Returns a DataFrame with ``Parameter``, ``Estimate``, ``StdErr``, ``t``/``Wald``,
    ``DF`` (#PSU - #strata) and ``Pr`` (p-value), plus ``OddsRatio`` for logistic.
    """
    import patsy

    d = _design(df, weight, strata, cluster)
    # patsy only recognises None/NaN as missing; pandas string/object columns may hold pd.NA
    for c in d.columns:
        if d[c].dtype == object or pd.api.types.is_string_dtype(d[c]):
            d[c] = d[c].astype(object).where(d[c].notna(), None)
    y, X = patsy.dmatrices(formula, d, return_type="dataframe", NA_action="drop")
    d = d.loc[y.index]
    w = d[weight].to_numpy(float)
    if domain is not None:
        inside = (d[domain] if isinstance(domain, str) else domain.reindex(d.index)).fillna(False).astype(bool)
        w = np.where(inside.to_numpy(), w, 0.0)
    else:
        inside = pd.Series(True, index=d.index)
    # Rescaling the weights does not change the estimates or the linearized
    # variance, but keeps the IRLS fit well conditioned (MEPS weights are large).
    scale = w[w > 0].mean()
    model = SurveyGLM(ModelType.LOGISTIC if family == "logistic" else ModelType.LINEAR)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        model.estimate(
            y=y.iloc[:, 0].to_numpy(float),
            x=X.to_numpy(float),
            x_labels=list(X.columns),
            samp_weight=w / scale,
            stratum=d[strata].to_numpy() if strata else None,
            psu=d[cluster].to_numpy() if cluster else None,
            single_psu=SinglePSUEst.skip,
        )
    cov = np.asarray(model.beta_cov)
    if family != "logistic":
        cov = cov  # samplics already uses the response residuals for the linear model
    n, p = int(inside.sum()), X.shape[1]
    if vadjust:
        cov = cov * (n - 1) / (n - p)
    est = np.asarray(model.beta["point_est"], dtype=float)
    se = np.sqrt(np.diag(cov))
    ddf = _df_design(d, strata, cluster)
    out = pd.DataFrame({"Parameter": X.columns, "Estimate": est, "StdErr": se})
    if family == "logistic":
        out["Wald"] = (est / se) ** 2
        out["Pr"] = stats.chi2.sf(out["Wald"], 1)
        out["OddsRatio"] = np.exp(est)
        z = stats.norm.ppf(0.975)
        out["OR_Lower"] = np.exp(est - z * se)
        out["OR_Upper"] = np.exp(est + z * se)
    else:
        out["t"] = est / se
        out["DF"] = ddf
        out["Pr"] = 2 * stats.t.sf(np.abs(out["t"]), ddf)
    out.attrs.update(nobs=n, nparams=p, df=ddf, cov=cov)
    return out
