"""
Survey estimation layer using samplics (Python) for complex survey analysis.

This module handles the weighted estimation that PySpark cannot do natively:
- Weighted means, totals, proportions
- Standard errors using Taylor Series Linearization
- Domain (subpopulation) estimation

PySpark has no native equivalent to:
- SAS PROC SURVEYMEANS / PROC SURVEYFREQ
- R survey::svydesign / svymean / svytotal
- Stata svy: mean / total

Survey design variables:
- VARSTR: strata variable
- VARPSU: cluster (PSU) variable
- PERWT**F: person-level weight (year-specific)
"""

import pandas as pd
import numpy as np

try:
    from samplics.estimation import TaylorEstimator
    from samplics.utils.types import PopParam
    HAS_SAMPLICS = True
except ImportError:
    HAS_SAMPLICS = False


def estimate_mean(
    df: pd.DataFrame,
    y_var: str,
    weight_var: str,
    strata_var: str = "VARSTR",
    cluster_var: str = "VARPSU",
    domain_var: str = None
) -> pd.DataFrame:
    """
    Compute survey-weighted mean with standard error.

    Parameters
    ----------
    df : pd.DataFrame
        Person-level data (output of PySpark ETL, collected to pandas)
    y_var : str
        Variable to estimate mean for
    weight_var : str
        Person weight variable (e.g., PERWT16F)
    strata_var : str
        Strata variable (default: VARSTR)
    cluster_var : str
        Cluster/PSU variable (default: VARPSU)
    domain_var : str, optional
        Domain variable for subpopulation analysis

    Returns
    -------
    pd.DataFrame with columns: estimate, std_error, [domain]
    """
    if not HAS_SAMPLICS:
        return _fallback_estimate(df, y_var, weight_var, "mean", domain_var)

    estimator = TaylorEstimator(PopParam.mean)

    y = df[y_var].values.astype(float)
    weights = df[weight_var].values.astype(float)
    strata = df[strata_var].values
    psu = df[cluster_var].values

    if domain_var is not None:
        domain = df[domain_var].values
        estimator.estimate(
            y=y, samp_weight=weights,
            stratum=strata, psu=psu,
            domain=domain, remove_nan=True
        )
    else:
        estimator.estimate(
            y=y, samp_weight=weights,
            stratum=strata, psu=psu,
            remove_nan=True
        )

    return estimator.to_dataframe()


def estimate_total(
    df: pd.DataFrame,
    y_var: str,
    weight_var: str,
    strata_var: str = "VARSTR",
    cluster_var: str = "VARPSU",
    domain_var: str = None
) -> pd.DataFrame:
    """
    Compute survey-weighted total with standard error.

    Parameters are the same as estimate_mean.
    """
    if not HAS_SAMPLICS:
        return _fallback_estimate(df, y_var, weight_var, "total", domain_var)

    estimator = TaylorEstimator(PopParam.total)

    y = df[y_var].values.astype(float)
    weights = df[weight_var].values.astype(float)
    strata = df[strata_var].values
    psu = df[cluster_var].values

    if domain_var is not None:
        domain = df[domain_var].values
        estimator.estimate(
            y=y, samp_weight=weights,
            stratum=strata, psu=psu,
            domain=domain, remove_nan=True
        )
    else:
        estimator.estimate(
            y=y, samp_weight=weights,
            stratum=strata, psu=psu,
            remove_nan=True
        )

    return estimator.to_dataframe()


def _fallback_estimate(
    df: pd.DataFrame,
    y_var: str,
    weight_var: str,
    param_type: str,
    domain_var: str = None
) -> pd.DataFrame:
    """
    Fallback weighted estimation when samplics is not available.
    Computes weighted mean/total without complex survey SE adjustment.
    """
    if domain_var is not None:
        results = []
        for domain_val, group in df.groupby(domain_var):
            w = group[weight_var].values
            y = group[y_var].values
            if param_type == "mean":
                est = np.average(y, weights=w)
            else:
                est = np.sum(y * w)
            results.append({
                "domain": domain_val,
                "estimate": est,
                "std_error": np.nan  # Cannot compute complex survey SE without samplics
            })
        return pd.DataFrame(results)
    else:
        w = df[weight_var].values
        y = df[y_var].values
        if param_type == "mean":
            est = np.average(y, weights=w)
        else:
            est = np.sum(y * w)
        return pd.DataFrame([{"estimate": est, "std_error": np.nan}])
