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
from scipy import stats

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


def _domain_lonely_variance(
    est: TaylorEstimator,
    y: np.ndarray,
    w: np.ndarray,
    x: np.ndarray,
    stratum: np.ndarray,
    psu: np.ndarray,
    n_dom: int,
    n_psu: pd.Series,
) -> float:
    """Extra variance under R ``survey.adjust.domain.lonely = TRUE`` with "adjust".

    For a stratum with n_h > 1 PSUs in which the domain (the first ``n_dom`` rows) has
    records in only one PSU, R skips centering, giving T^2 * n_h / (n_h - 1) instead of
    the usual T^2 (T = that PSU's score total). Returns the difference, T^2 / (n_h - 1).
    """
    dom_psus = pd.Series(psu[:n_dom]).groupby(stratum[:n_dom]).nunique()
    single = dom_psus.index[(dom_psus == 1).to_numpy() & (n_psu.reindex(dom_psus.index) > 1).to_numpy()]
    if not len(single):
        return 0.0
    scores = est._score_variable(y, w, x)[:, 0]
    mask = np.isin(stratum, single)
    totals = pd.Series(scores[mask]).groupby(stratum[mask]).sum()
    return float((totals**2 / (n_psu.reindex(totals.index) - 1)).sum())


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
    adjust_domain_lonely: bool = False,
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
    adjust_domain_lonely : also treat strata where a domain/subpopulation has a single PSU
        as lonely (R ``options(survey.adjust.domain.lonely = TRUE)``). With
        ``lonely_psu="adjust"`` that PSU's score total is not centered, which inflates the
        stratum's contribution by n_h / (n_h - 1).

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
    n_psu = pd.Series(psu_id).groupby(strat).nunique()
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
            if adjust_domain_lonely and lonely_psu == "adjust" and d is not None:
                var_ = var_ + _domain_lonely_variance(est, y_d, w_d, x_d, s_d, p_d, n_dom, n_psu)
            row = {"variable": var, "estimate": float(pt), "se": float(np.sqrt(var_))}
            if domain:
                row.update(dict(zip(domain, d)))
            rows.append(row)

    cols = (domain or []) + ["variable", "estimate", "se"]
    return pd.DataFrame(rows, columns=cols)


def _qrule_math(x: np.ndarray, w: np.ndarray, p: float) -> float:
    """R survey ``qrule="math"``: inverse of the weighted empirical CDF."""
    keep = w > 0
    x, w = x[keep], w[keep]
    if not x.size:
        return np.nan
    order = np.argsort(x, kind="stable")
    x, cumw = x[order], np.cumsum(w[order])
    total = cumw[-1]
    below = np.nonzero(cumw <= p * total)[0]
    pos = below[-1] if below.size else 0
    if below.size and p == cumw[pos] / total:
        return float(x[pos])
    return float(x[min(pos + 1, len(x) - 1)] if below.size else x[0])


def survey_quantile(
    df: pd.DataFrame,
    variable: str,
    weight: str,
    quantile: float = 0.5,
    stratum: str = "VARSTR",
    psu: str = "VARPSU",
    domain: str | Sequence[str] | None = None,
    subpop: pd.Series | None = None,
    alpha: float = 0.05,
    lonely_psu: str = "adjust",
) -> pd.DataFrame:
    """Design-based quantile with a Woodruff confidence-interval SE.

    Matches R ``svyquantile(~x, design, quantiles = q)`` (survey >= 4.0 defaults:
    ``qrule = "math"``, ``interval.type = "mean"``, ``df = degf(domain design)``) and the
    ``svyby(..., FUN = svyquantile)`` form. SE = CI half-width / t quantile, as in R.
    """
    domain = [domain] if isinstance(domain, str) else (list(domain) if domain else None)
    codes, labels = _domain_labels(df, domain, subpop) or (np.zeros(len(df), dtype=np.int64), [()])
    x_all = df[variable].to_numpy(dtype=float)
    w_all = df[weight].to_numpy(dtype=float)
    strata = df[stratum].astype(str).to_numpy(dtype=object)
    psu_id = (df[stratum].astype(str) + "_" + df[psu].astype(str)).to_numpy(dtype=object)

    rows = []
    for k, label in enumerate(labels):
        in_dom = codes == k
        if not in_dom.any():
            continue
        x, w = x_all[in_dom], w_all[in_dom]
        if not (w > 0).any():  # all-zero-weight domain: omitted, as in survey_estimate
            continue
        qhat = _qrule_math(x, w, quantile)
        # R degf(): PSUs minus strata among records with nonzero weight in the domain
        inset = in_dom & (w_all != 0)
        t_crit = stats.t.ppf(1 - alpha / 2, len(set(psu_id[inset])) - len(set(strata[inset])))
        tmp = df[[weight, stratum, psu]].assign(_below=(x_all <= qhat).astype(float))
        m = survey_estimate(tmp, "_below", weight=weight, stat="mean", stratum=stratum, psu=psu,
                            subpop=pd.Series(in_dom, index=df.index), lonely_psu=lonely_psu)
        p_hat, p_se = m["estimate"].iloc[0], m["se"].iloc[0]
        p_lo, p_hi = p_hat - t_crit * p_se, p_hat + t_crit * p_se
        lo = _qrule_math(x, w, p_lo) if p_lo >= 0 else np.nan
        hi = _qrule_math(x, w, p_hi) if p_hi <= 1 else np.nan
        row = {"variable": variable, "estimate": qhat, "se": (hi - lo) / (2 * t_crit)}
        row.update(dict(zip(domain or [], label)))
        rows.append(row)
    return pd.DataFrame(rows, columns=(domain or []) + ["variable", "estimate", "se"])


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


# Regression ------------------------------------------------------------------


def _score_total_vcov(scores: np.ndarray, stratum: np.ndarray, psu: np.ndarray, lonely_psu: str) -> np.ndarray:
    """With-replacement (Taylor) covariance of the population total of row-level scores.

    Equivalent to R ``svyrecvar`` for a one-stage design (``nest=TRUE``, no fpc):
    PSU totals are centered within stratum and scaled by n_h / (n_h - 1). Singleton
    strata follow ``survey.lonely.psu``: "adjust" centers them at the mean of all PSU
    totals, "remove" drops them, "fail" raises.
    """
    tot = pd.DataFrame(scores).groupby([stratum, psu]).sum()
    strata = tot.index.get_level_values(0).to_numpy()
    z = tot.to_numpy()
    grand = z.mean(axis=0)
    p = z.shape[1]
    vcov = np.zeros((p, p))
    for s in pd.unique(strata):
        zs = z[strata == s]
        n_h = len(zs)
        if n_h == 1:
            if lonely_psu == "fail":
                raise ValueError(f"stratum {s} has only one PSU")
            if lonely_psu == "adjust":
                d = zs - grand
                vcov += d.T @ d
            continue
        d = zs - zs.mean(axis=0)
        vcov += n_h / (n_h - 1) * (d.T @ d)
    return vcov


def _irls_logit(x: np.ndarray, y: np.ndarray, w: np.ndarray, max_iter: int) -> tuple[np.ndarray, np.ndarray]:
    """Weighted logistic fit replicating R ``glm.fit`` (as called by ``svyglm``).

    Mirrors R step for step so results agree to rounding: weights rescaled to mean 1,
    ``mustart = (w*y + 0.5)/(w + 1)``, convergence on relative deviance change < 1e-8,
    and R's final working weights (from the start of the last iteration). Returns the
    coefficients and the per-row influence scores ``estfun %*% (X'WX)^-1``.
    """
    pw = w / w.mean()
    good = pw > 0
    xg, yg, pwg = x[good], y[good], pw[good]

    def deviance(mu):
        with np.errstate(divide="ignore", invalid="ignore"):
            d = np.where(yg > 0, yg * np.log(yg / mu), 0) + np.where(yg < 1, (1 - yg) * np.log((1 - yg) / (1 - mu)), 0)
        return 2 * np.sum(pwg * d)

    mu = (pwg * yg + 0.5) / (pwg + 1)
    eta = np.log(mu / (1 - mu))
    dev_old = deviance(mu)
    for _ in range(max_iter):
        mu_eta = mu * (1 - mu)
        z = eta + (yg - mu) / mu_eta
        wts = pwg * mu_eta
        sw = np.sqrt(wts)
        q, r = np.linalg.qr(xg * sw[:, None])  # least squares via QR, as R's lm.fit
        beta = np.linalg.solve(r, q.T @ (sw * z))
        eta = xg @ beta
        mu = 1 / (1 + np.exp(-eta))
        dev = deviance(mu)
        if abs(dev - dev_old) / (abs(dev) + 0.1) < 1e-8:
            break
        dev_old = dev
    scores = np.zeros_like(x)
    r_inv = np.linalg.inv(r)
    scores[good] = (xg * (wts * (yg - mu) / (mu * (1 - mu)))[:, None]) @ (r_inv @ r_inv.T)
    return beta, scores


def survey_glm(
    df: pd.DataFrame,
    outcome: str,
    weight: str,
    numeric: Sequence[str] = (),
    factors: Sequence[str] = (),
    stratum: str = "VARSTR",
    psu: str = "VARPSU",
    subpop: pd.Series | None = None,
    lonely_psu: str = "adjust",
    max_iter: int = 50,
) -> pd.DataFrame:
    """Design-based logistic regression, matching R ``svyglm(..., family = quasibinomial)``.

    ``factors`` are expanded to treatment dummies (lowest level as reference) named as R
    does for ``as.factor(X)``, e.g. ``as.factor(SEX)2``. Rows outside ``subpop`` keep their
    stratum/PSU in the design with zero weight (R ``subset(design, ...)``). Standard
    errors use the linearization sandwich; t tests use ``degf(design) + 1 - p`` df, as in
    ``summary.svyglm``. Returns columns ``variable, estimate, se, t, p``.
    """
    keep = np.ones(len(df), dtype=bool) if subpop is None else subpop.to_numpy(dtype=bool)
    # complete cases only (R's model frame drops NAs); dropped rows stay in the design with weight 0
    keep &= df[[outcome, *numeric, *factors]].notna().all(axis=1).to_numpy()
    w = np.where(keep, df[weight].to_numpy(dtype=float), 0.0)
    cols, names = [np.ones(len(df))], ["(Intercept)"]
    for v in numeric:
        cols.append(np.where(keep, df[v].to_numpy(dtype=float), 0.0))
        names.append(v)
    for f in factors:
        levels = sorted(pd.unique(df.loc[keep & (w > 0), f]))
        for lev in levels[1:]:
            cols.append((df[f] == lev).to_numpy(dtype=float))
            names.append(f"as.factor({f}){_fmt_level(lev)}")
    x = np.column_stack(cols)
    y = np.where(keep, df[outcome].to_numpy(dtype=float), 0.0)

    beta, scores = _irls_logit(x, y, w, max_iter)
    strata = df[stratum].astype(str).to_numpy(dtype=object)
    psu_id = df[psu].astype(str).to_numpy(dtype=object)
    vcov = _score_total_vcov(scores, strata, psu_id, lonely_psu)
    se = np.sqrt(np.diag(vcov))

    inset = w != 0
    degf = len(set(zip(strata[inset], psu_id[inset]))) - len(set(strata[inset]))
    t = beta / se
    p = 2 * stats.t.sf(np.abs(t), degf + 1 - len(beta))
    return pd.DataFrame({"variable": names, "estimate": beta, "se": se, "t": t, "p": p})
