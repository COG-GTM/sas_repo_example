"""Survey estimation utilities for MEPS PySpark Migration.

PySpark has no native equivalent of SAS PROC SURVEYMEANS / R survey::svydesign.
This module provides a bridge to perform survey-weighted estimation using
the survey design variables (VARSTR, VARPSU, PERWTxxF) that are preserved
in the PySpark ETL output Parquet files.

Two estimation backends are supported:
  1. Python-native: using pandas + numpy for basic weighted estimates
  2. R via rpy2: using the R 'survey' package for full Taylor-series
     linearization variance estimation (preferred for publication-quality results)
"""

from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


def weighted_mean(
    df: pd.DataFrame,
    var_col: str,
    weight_col: str,
) -> Tuple[float, float]:
    """Compute a weighted mean and approximate standard error.

    This is a simplified estimator; for correct complex-survey SEs,
    use the R survey backend via `survey_estimate_r()`.

    Args:
        df: pandas DataFrame with analysis data.
        var_col: Name of the analysis variable.
        weight_col: Name of the weight column.

    Returns:
        Tuple of (weighted_mean, approximate_se).
    """
    weights = df[weight_col].values.astype(float)
    values = df[var_col].values.astype(float)

    mask = ~(np.isnan(values) | np.isnan(weights))
    weights = weights[mask]
    values = values[mask]

    total_weight = np.sum(weights)
    if total_weight == 0:
        return (0.0, 0.0)

    w_mean = np.sum(weights * values) / total_weight

    # Approximate SE using weighted variance (not accounting for complex design)
    w_var = np.sum(weights * (values - w_mean) ** 2) / total_weight
    n_eff = total_weight**2 / np.sum(weights**2)
    approx_se = np.sqrt(w_var / n_eff) if n_eff > 0 else 0.0

    return (float(w_mean), float(approx_se))


def weighted_total(
    df: pd.DataFrame,
    var_col: str,
    weight_col: str,
) -> Tuple[float, float]:
    """Compute a weighted total and approximate standard error.

    Args:
        df: pandas DataFrame with analysis data.
        var_col: Name of the analysis variable.
        weight_col: Name of the weight column.

    Returns:
        Tuple of (weighted_total, approximate_se).
    """
    weights = df[weight_col].values.astype(float)
    values = df[var_col].values.astype(float)

    mask = ~(np.isnan(values) | np.isnan(weights))
    weights = weights[mask]
    values = values[mask]

    w_total = np.sum(weights * values)

    # Approximate SE
    w_mean = np.mean(values)
    w_var = np.sum(weights**2 * (values - w_mean) ** 2)
    approx_se = np.sqrt(w_var)

    return (float(w_total), float(approx_se))


def weighted_proportion(
    df: pd.DataFrame,
    var_col: str,
    weight_col: str,
    domain_col: Optional[str] = None,
    domain_val: Optional[object] = None,
) -> Dict[str, Tuple[float, float]]:
    """Compute weighted proportions for a categorical variable.

    Args:
        df: pandas DataFrame.
        var_col: Name of the categorical variable.
        weight_col: Name of the weight column.
        domain_col: Optional domain/subpopulation column.
        domain_val: Value of domain_col that defines the subpopulation.

    Returns:
        Dictionary mapping category values to (proportion, approx_se) tuples.
    """
    if domain_col is not None and domain_val is not None:
        df = df[df[domain_col] == domain_val]

    total_weight = df[weight_col].sum()
    if total_weight == 0:
        return {}

    results = {}
    for val in df[var_col].unique():
        sub = df[df[var_col] == val]
        prop = sub[weight_col].sum() / total_weight
        # Approximate binomial SE
        n_eff = total_weight**2 / (df[weight_col] ** 2).sum()
        se = np.sqrt(prop * (1 - prop) / n_eff) if n_eff > 0 else 0.0
        results[val] = (float(prop), float(se))

    return results


def survey_estimate_r(
    df: pd.DataFrame,
    formula: str,
    strata_col: str = "VARSTR",
    cluster_col: str = "VARPSU",
    weight_col: str = "PERWT20F",
    estimation_type: str = "mean",
    domain_formula: Optional[str] = None,
) -> pd.DataFrame:
    """Run survey estimation using R's survey package via rpy2.

    This is the recommended method for publication-quality estimates.
    Requires rpy2 and the R 'survey' package to be installed.

    Args:
        df: pandas DataFrame with analysis data.
        formula: R formula string (e.g., '~TOTEXP16').
        strata_col: Name of the strata column (default: VARSTR).
        cluster_col: Name of the cluster/PSU column (default: VARPSU).
        weight_col: Name of the weight column.
        estimation_type: One of 'mean', 'total', 'proportion'.
        domain_formula: Optional R formula for domain analysis.

    Returns:
        pandas DataFrame with estimation results.
    """
    try:
        import rpy2.robjects as ro
        from rpy2.robjects import pandas2ri
        from rpy2.robjects.packages import importr

        pandas2ri.activate()
        survey = importr("survey")

        r_df = pandas2ri.py2rpy(df)
        ro.globalenv["data"] = r_df

        ro.r('options(survey.lonely.psu="adjust")')

        design_cmd = (
            f'svydesign(id=~{cluster_col}, strata=~{strata_col}, '
            f'weights=~{weight_col}, data=data, nest=TRUE)'
        )
        ro.r(f"dsgn <- {design_cmd}")

        if estimation_type == "mean":
            if domain_formula:
                result = ro.r(f'svyby({formula}, by={domain_formula}, FUN=svymean, design=dsgn)')
            else:
                result = ro.r(f'svymean({formula}, design=dsgn)')
        elif estimation_type == "total":
            if domain_formula:
                result = ro.r(f'svyby({formula}, by={domain_formula}, FUN=svytotal, design=dsgn)')
            else:
                result = ro.r(f'svytotal({formula}, design=dsgn)')
        else:
            result = ro.r(f'svymean({formula}, design=dsgn)')

        return pandas2ri.rpy2py(ro.r("as.data.frame")(result))

    except ImportError:
        # Fallback: return empty DataFrame with a warning
        print(
            "WARNING: rpy2 not available. Install rpy2 and R 'survey' package "
            "for publication-quality survey estimates. Falling back to "
            "Python approximate estimates."
        )
        return pd.DataFrame()
