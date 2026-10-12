"""Shared helpers for loading and analyzing MEPS-HC data in Python.

These helpers establish the patterns used by every script in the Python/ track:

* ``read_meps``      -- load MEPS public use files (.sas7bdat, SAS XPORT .ssp/.xpt, .dta)
* ``survey_estimate`` -- design-based (Taylor linearization) totals, means, and ratios
  using ``samplics``, with domain (subpopulation) estimation and R-compatible
  lonely-PSU handling.

See Python/README.md for details.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Sequence
from pathlib import Path

import numpy as np
import pandas as pd
from samplics.estimation import TaylorEstimator
from samplics.utils.formats import fpc_as_dict
from samplics.utils.types import PopParam

MEPS_DIR = os.environ.get("MEPS_DIR", "C:/MEPS")

_OUT_OF_DOMAIN = "__out_of_domain__"
_PARAMS = {"total": PopParam.total, "mean": PopParam.mean, "ratio": PopParam.ratio}


# Data loading -----------------------------------------------------------------


def meps_path(filename: str, meps_dir: str | None = None) -> str:
    """Return the full path of a MEPS file in ``meps_dir`` (default: $MEPS_DIR or C:/MEPS)."""
    return str(Path(meps_dir or MEPS_DIR) / filename)


def read_meps(path: str, columns: Iterable[str] | None = None) -> pd.DataFrame:
    """Load a MEPS public use file into a pandas DataFrame.

    Python equivalent of SAS ``DATA ...; SET "x.sas7bdat"`` and ``PROC XCOPY ... IMPORT``
    (XPORT .ssp files). Supported formats:

    * ``.sas7bdat`` -- SAS V9 (data years 2017+)
    * ``.ssp`` / ``.xpt`` -- SAS XPORT transport (data years 1996-2016). SAS CPORT
      .ssp files (most 2017+ files) cannot be read in Python; use .sas7bdat or .dta.
    * ``.dta`` -- Stata

    Column names are upper-cased (XPORT and V9 files can differ in case) and SAS
    character columns are decoded to ``str``.
    """
    ext = Path(path).suffix.lower()
    if ext == ".sas7bdat":
        df = pd.read_sas(path, format="sas7bdat", encoding="latin-1")
    elif ext in (".ssp", ".xpt", ".xport"):
        df = pd.read_sas(path, format="xport", encoding="latin-1")
    elif ext == ".dta":
        df = pd.read_stata(path, convert_categoricals=False)
    else:
        raise ValueError(f"Unsupported MEPS file type: {path}")

    df.columns = [c.upper() for c in df.columns]
    if ext in (".ssp", ".xpt", ".xport"):
        # pandas decodes some IBM-float zeros in XPORT files as 16**-65 (~5.4e-79)
        # rather than 0 (R foreign::read.xport and SAS read them as 0)
        num = df.select_dtypes("number").columns
        df[num] = df[num].mask(df[num].abs() < 1e-70, 0.0)
    for col in df.columns[df.dtypes == object]:
        df[col] = df[col].map(lambda v: v.decode("latin-1") if isinstance(v, bytes) else v)
        df[col] = df[col].str.strip() if df[col].map(type).eq(str).all() else df[col]
    if columns is not None:
        df = df[[c.upper() for c in columns]]
    return df


# Survey estimation ------------------------------------------------------------


def _domain_labels(
    df: pd.DataFrame, domain: Sequence[str] | None, subpop: pd.Series | None
) -> tuple[np.ndarray, list[tuple]] | None:
    """Integer domain code per row (-1 = excluded) and the domain level tuples.

    Rows with a missing domain value or outside ``subpop`` get code -1: they are kept in the
    design (zero contribution) but no estimate is reported for them, like R ``svyby``.
    """
    if domain is None and subpop is None:
        return None
    if domain:
        keys = pd.Series(list(zip(*(df[c].astype(object).to_numpy() for c in domain))), index=df.index)
        codes, uniques = pd.factorize(keys)
        labels = [tuple(_fmt_level(v) for v in u) for u in uniques]
        codes = np.where(df[list(domain)].isna().any(axis=1).to_numpy(), -1, codes)
    else:
        codes, labels = np.zeros(len(df), dtype=np.int64), [()]
    if subpop is not None:
        codes = np.where(np.asarray(subpop, dtype=bool), codes, -1)
    return codes, labels


def _fmt_level(value) -> str:
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _lonely_strata(stratum: np.ndarray, psu: np.ndarray) -> np.ndarray:
    pairs = pd.DataFrame({"s": stratum, "p": psu}).drop_duplicates()
    counts = pairs.groupby("s")["p"].size()
    return counts.index[counts == 1].to_numpy()


def _adjust_variance(
    est: TaylorEstimator,
    y: np.ndarray,
    w: np.ndarray,
    x: np.ndarray,
    stratum: np.ndarray,
    lonely: np.ndarray,
) -> float:
    """Variance contribution of lonely-PSU strata under R's survey.lonely.psu = "adjust".

    R centers a singleton stratum's PSU score total at the grand mean (0 for
    linearized scores), i.e. the contribution is the squared PSU score total.
    """
    scores = est._score_variable(y, w, x)[:, 0]
    mask = np.isin(stratum, lonely)
    sums = pd.Series(scores[mask]).groupby(stratum[mask]).sum()
    return float((sums**2).sum())


def _domain_blocks(dom: tuple[np.ndarray, list[tuple]] | None, n: int, psu_rows: np.ndarray):
    """Yield ``(label, row_index)`` for each domain.

    Domain estimation is done by giving out-of-domain records a zero contribution
    (weight 0) while keeping every stratum/PSU in the design. Out-of-domain records
    within a PSU all contribute exactly zero, so they are collapsed to one zero-weight
    placeholder per PSU (``psu_rows``). This gives identical estimates and SEs to using
    all rows, but is much faster when there are many domains (e.g. 600+ drug names).
    """
    if dom is None:
        yield None, np.arange(n)
        return
    codes, labels = dom
    order = np.argsort(codes, kind="stable")
    bounds = np.searchsorted(codes[order], np.arange(len(labels) + 1))
    for k, label in enumerate(labels):
        idx = order[bounds[k]:bounds[k + 1]]
        if idx.size:
            yield label, idx


def survey_estimate(
    df: pd.DataFrame,
    variables: str | Sequence[str],
    weight: str,
    stat: str = "total",
    stratum: str = "VARSTR",
    psu: str = "VARPSU",
    domain: str | Sequence[str] | None = None,
    subpop: pd.Series | None = None,
    denominator: str | None = None,
    lonely_psu: str = "adjust",
) -> pd.DataFrame:
    """Design-based survey estimates with Taylor-series linearization (samplics).

    Equivalent to SAS ``PROC SURVEYMEANS`` with ``STRATA``/``CLUSTER``/``WEIGHT``/``DOMAIN``
    and to R ``svyby(..., FUN = svytotal/svymean/svyratio)`` on
    ``svydesign(id = ~VARPSU, strata = ~VARSTR, weights = ~PERWTyyF, nest = TRUE)``.

    Parameters
    ----------
    df : person- (or event-) level data. **Do not subset it** -- all rows are kept so
        that the variance reflects the full sample design.
    variables : analysis variable(s).
    weight : survey weight column (e.g. ``PERWT19F``).
    stat : ``"total"``, ``"mean"``, or ``"ratio"`` (ratio of ``variables`` to ``denominator``).
    domain : column(s) defining domains (SAS ``DOMAIN a*b``; R ``svyby(by = ~a + b)``).
    subpop : boolean mask defining the analytic subpopulation (SAS ``DOMAIN flag('1')``;
        R ``subset(design, ...)``). Rows outside it are kept for variance estimation
        but excluded from the output.
    lonely_psu : ``"adjust"`` (default, matches R ``options(survey.lonely.psu = "adjust")``),
        ``"remove"`` (skip singleton strata, as SAS does), or ``"fail"``.

    Returns
    -------
    Tidy DataFrame with one row per domain x variable and columns
    ``[*domain, variable, estimate, se]``.
    """
    variables = [variables] if isinstance(variables, str) else list(variables)
    domain = [domain] if isinstance(domain, str) else (list(domain) if domain else None)
    if stat not in _PARAMS:
        raise ValueError(f"stat must be one of {list(_PARAMS)}")
    if stat == "ratio" and denominator is None:
        raise ValueError("denominator is required for stat='ratio'")

    w = df[weight].to_numpy(dtype=float)
    # nest = TRUE: PSU codes are only unique within strata
    strat = df[stratum].astype(str).to_numpy(dtype=object)
    psu_id = (df[stratum].astype(str) + "_" + df[psu].astype(str)).to_numpy(dtype=object)
    x = df[denominator].to_numpy(dtype=float) if denominator else np.array(None)
    dom = _domain_labels(df, domain, subpop)

    lonely = _lonely_strata(strat, psu_id)
    if lonely.size:
        if lonely_psu == "fail":
            raise ValueError(f"Strata with a single PSU: {list(lonely)}")
        if lonely_psu not in ("adjust", "remove"):
            raise ValueError("lonely_psu must be 'adjust', 'remove', or 'fail'")
    # Singleton strata are skipped by samplics; "adjust" adds R's contribution back below
    skipped = lonely if lonely.size else np.array(None)
    fpc = fpc_as_dict(strat, 1)

    # one zero-weight placeholder record per PSU (see _domain_blocks)
    psu_rows = pd.Series(psu_id).drop_duplicates().index.to_numpy()
    blocks = list(_domain_blocks(dom, len(df), psu_rows))
    has_x = denominator is not None

    rows = []
    for var in variables:
        y_all = df[var].to_numpy(dtype=float)
        est = TaylorEstimator(param=_PARAMS[stat])
        for d, idx in blocks:
            if d is not None and not w[idx].any():
                # like SAS and R, records with zero weight are out of the analysis
                continue
            if d is not None:
                idx = np.concatenate([idx, psu_rows])
            n_dom = len(idx) - (0 if d is None else len(psu_rows))
            w_d = w[idx].copy()
            w_d[n_dom:] = 0.0
            y_d, s_d, p_d = y_all[idx].copy(), strat[idx], psu_id[idx]
            y_d[n_dom:] = 0.0
            x_d = x[idx].copy() if has_x else x
            if has_x:
                x_d[n_dom:] = 0.0
            # samplics' Taylor-linearization point/variance estimators. These are called
            # directly (rather than est.estimate()) because estimate() also computes CVs
            # and fails with ZeroDivisionError on domains whose estimate is 0.
            pt = est._get_point(y=y_d, samp_weight=w_d, x=x_d, domain=np.array(None))
            var_, _ = est._get_variance(
                y=y_d, samp_weight=w_d, x=x_d, stratum=s_d, psu=p_d,
                ssu=np.array(None), domain=np.array(None), fpc=fpc,
                skipped_strata=skipped,
            )
            if lonely.size and lonely_psu == "adjust":
                var_ = var_ + _adjust_variance(est, y_d, w_d, x_d, s_d, lonely)
            row = {"variable": var, "estimate": float(pt), "se": float(np.sqrt(var_))}
            if domain:
                row.update(dict(zip(domain, d)))
            rows.append(row)

    cols = (domain or []) + ["variable", "estimate", "se"]
    return pd.DataFrame(rows, columns=cols)


def add_dummies(df: pd.DataFrame, col: str, levels: Sequence | None = None) -> list[str]:
    """Add 0/1 indicator columns ``f"{col}{level}"`` for each level of a categorical column.

    Equivalent to a factor variable in R ``svytotal(~factor)`` (column names match R's) and to
    a class variable in SAS ``PROC SURVEYMEANS``. Returns the new column names.
    """
    levels = list(levels) if levels is not None else list(pd.Series(df[col]).dropna().unique())
    names = []
    for level in levels:
        name = f"{col}{level}"
        df[name] = (df[col] == level).astype(int)
        names.append(name)
    return names


def wide_estimates(results: pd.DataFrame, domain: str | Sequence[str]) -> pd.DataFrame:
    """Pivot ``survey_estimate`` output to one row per domain (like R ``svyby`` output)."""
    domain = [domain] if isinstance(domain, str) else list(domain)
    wide = results.pivot_table(index=domain, columns="variable", values=["estimate", "se"])
    wide.columns = [f"{v}" if s == "estimate" else f"se.{v}" for s, v in wide.columns]
    return wide.reset_index()
