"""
MEPS Analysis Module

Survey statistics functions for MEPS data analysis.
Equivalent to R's survey package functions (svytotal, svymean, svyby, svyglm, etc.)
and SAS SURVEY procedures (PROC SURVEYMEANS, PROC SURVEYREG, etc.).
"""

from typing import Any, Callable, Dict, List, Optional, Union

import numpy as np
import pandas as pd
from scipy import stats

from meps.survey_design import MEPSSurveyDesign


def svytotal(
    design: MEPSSurveyDesign,
    variables: Union[str, List[str]],
) -> pd.DataFrame:
    """
    Calculate population totals.
    Equivalent to R's svytotal() and Stata's svy: total.

    Args:
        design: MEPSSurveyDesign object
        variables: Variable name(s) to calculate totals for

    Returns:
        DataFrame with estimates, standard errors, and confidence intervals
    """
    if isinstance(variables, str):
        variables = [variables]

    results = []
    for var in variables:
        result = design.weighted_total(var)
        results.append({
            "variable": var,
            "total": result["estimate"],
            "SE": result["se"],
            "ci_lower": result["ci"][0],
            "ci_upper": result["ci"][1],
        })

    return pd.DataFrame(results)


def svymean(
    design: MEPSSurveyDesign,
    variables: Union[str, List[str]],
) -> pd.DataFrame:
    """
    Calculate weighted means and proportions.
    Equivalent to R's svymean() and Stata's svy: mean.

    Args:
        design: MEPSSurveyDesign object
        variables: Variable name(s) to calculate means for

    Returns:
        DataFrame with estimates, standard errors, and confidence intervals
    """
    if isinstance(variables, str):
        variables = [variables]

    results = []
    for var in variables:
        result = design.weighted_mean(var)
        results.append({
            "variable": var,
            "mean": result["estimate"],
            "SE": result["se"],
            "ci_lower": result["ci"][0],
            "ci_upper": result["ci"][1],
        })

    return pd.DataFrame(results)


def svyby(
    design: MEPSSurveyDesign,
    variables: Union[str, List[str]],
    by_var: str,
    func: Callable,
) -> pd.DataFrame:
    """
    Calculate estimates by subgroups.
    Equivalent to R's svyby().

    Args:
        design: MEPSSurveyDesign object
        variables: Variable name(s) to analyze
        by_var: Variable to group by
        func: Function to apply (svytotal or svymean)

    Returns:
        DataFrame with estimates by group
    """
    if isinstance(variables, str):
        variables = [variables]

    groups = design.data[by_var].unique()
    groups = [g for g in groups if pd.notna(g)]

    all_results = []
    for group in sorted(groups):
        group_condition = design.data[by_var] == group
        group_design = design.subset(group_condition)

        group_result = func(group_design, variables)
        group_result[by_var] = group
        all_results.append(group_result)

    result_df = pd.concat(all_results, ignore_index=True)

    cols = [by_var] + [c for c in result_df.columns if c != by_var]
    return result_df[cols]


def svyglm(
    design: MEPSSurveyDesign,
    formula: str,
    family: str = "gaussian",
) -> Dict[str, Any]:
    """
    Generalized linear models for survey data.
    Equivalent to R's svyglm() and SAS PROC SURVEYREG/SURVEYLOGISTIC.

    This implementation uses weighted least squares for linear models
    and weighted maximum likelihood for logistic models.

    Args:
        design: MEPSSurveyDesign object
        formula: Model formula (e.g., 'y ~ x1 + x2')
        family: Distribution family ('gaussian', 'binomial', 'quasibinomial')

    Returns:
        Dictionary with model results including coefficients, standard errors, etc.
    """
    import statsmodels.api as sm
    from statsmodels.genmod.generalized_linear_model import GLM
    from statsmodels.genmod import families

    parts = formula.replace(" ", "").split("~")
    if len(parts) != 2:
        raise ValueError("Formula must be in format 'y ~ x1 + x2'")

    y_var = parts[0]
    x_vars = parts[1].split("+")

    y = design.data[y_var].values
    weights = design.get_weights()

    X_parts = []
    var_names = ["Intercept"]

    for x_var in x_vars:
        x_var = x_var.strip()

        if x_var.startswith("as.factor(") or x_var.startswith("C("):
            var_name = x_var.replace("as.factor(", "").replace("C(", "").replace(")", "")
            dummies = pd.get_dummies(design.data[var_name], prefix=var_name, drop_first=True)
            X_parts.append(dummies.values)
            var_names.extend(dummies.columns.tolist())
        else:
            X_parts.append(design.data[x_var].values.reshape(-1, 1))
            var_names.append(x_var)

    X = np.hstack([np.ones((len(y), 1))] + X_parts)

    valid_mask = ~np.isnan(y) & ~np.any(np.isnan(X), axis=1) & (weights > 0)
    y = y[valid_mask]
    X = X[valid_mask]
    weights = weights[valid_mask]

    family_map = {
        "gaussian": families.Gaussian(),
        "binomial": families.Binomial(),
        "quasibinomial": families.Binomial(),
        "poisson": families.Poisson(),
    }

    glm_family = family_map.get(family, families.Gaussian())

    try:
        model = GLM(y, X, family=glm_family, freq_weights=weights)
        result = model.fit()

        coefficients = dict(zip(var_names, result.params))
        std_errors = dict(zip(var_names, result.bse))
        t_values = dict(zip(var_names, result.tvalues))
        p_values = dict(zip(var_names, result.pvalues))

        return {
            "coefficients": coefficients,
            "std_errors": std_errors,
            "t_values": t_values,
            "p_values": p_values,
            "n_obs": len(y),
            "deviance": result.deviance,
            "aic": result.aic if hasattr(result, "aic") else None,
            "summary": result.summary(),
        }
    except Exception as e:
        return {
            "error": str(e),
            "coefficients": {},
            "std_errors": {},
            "t_values": {},
            "p_values": {},
        }


def svyquantile(
    design: MEPSSurveyDesign,
    variable: str,
    quantiles: Union[float, List[float]] = 0.5,
) -> pd.DataFrame:
    """
    Calculate weighted quantiles (e.g., median).
    Equivalent to R's svyquantile().

    Args:
        design: MEPSSurveyDesign object
        variable: Variable name
        quantiles: Quantile(s) to calculate (default: 0.5 for median)

    Returns:
        DataFrame with quantile estimates
    """
    if isinstance(quantiles, (int, float)):
        quantiles = [quantiles]

    values = design.data[variable].values
    weights = design.get_weights()

    valid_mask = ~np.isnan(values)
    values = values[valid_mask]
    weights = weights[valid_mask]

    sorted_indices = np.argsort(values)
    sorted_values = values[sorted_indices]
    sorted_weights = weights[sorted_indices]

    cumulative_weights = np.cumsum(sorted_weights)
    total_weight = cumulative_weights[-1]
    cumulative_probs = cumulative_weights / total_weight

    results = []
    for q in quantiles:
        idx = np.searchsorted(cumulative_probs, q)
        if idx >= len(sorted_values):
            idx = len(sorted_values) - 1
        quantile_value = sorted_values[idx]

        results.append({
            "variable": variable,
            "quantile": q,
            "estimate": quantile_value,
        })

    return pd.DataFrame(results)


def svyratio(
    design: MEPSSurveyDesign,
    numerator: str,
    denominator: str,
) -> Dict[str, float]:
    """
    Calculate ratio statistics.
    Equivalent to R's svyratio().

    Args:
        design: MEPSSurveyDesign object
        numerator: Numerator variable name
        denominator: Denominator variable name

    Returns:
        Dictionary with ratio estimate and standard error
    """
    weights = design.get_weights()
    num_values = design.data[numerator].values
    den_values = design.data[denominator].values

    valid_mask = ~np.isnan(num_values) & ~np.isnan(den_values)
    weights = weights[valid_mask]
    num_values = num_values[valid_mask]
    den_values = den_values[valid_mask]

    num_total = np.sum(weights * num_values)
    den_total = np.sum(weights * den_values)
    ratio = num_total / den_total

    residuals = num_values - ratio * den_values
    design.data["_temp_ratio_resid"] = np.nan
    design.data.loc[valid_mask, "_temp_ratio_resid"] = residuals

    var_resid = design._calculate_variance("_temp_ratio_resid", "total")
    del design.data["_temp_ratio_resid"]

    se = np.sqrt(var_resid) / den_total

    return {
        "ratio": ratio,
        "SE": se,
        "ci_lower": ratio - 1.96 * se,
        "ci_upper": ratio + 1.96 * se,
    }


def domain_analysis(
    design: MEPSSurveyDesign,
    domain_var: str,
    analysis_func: Callable,
    **kwargs,
) -> pd.DataFrame:
    """
    Analyze subpopulation while maintaining survey structure.
    Equivalent to SAS DOMAIN statement and Stata subpop() option.

    Important: This keeps all observations but flags the domain - doesn't delete observations.
    This is critical for proper variance estimation.

    Args:
        design: MEPSSurveyDesign object
        domain_var: Variable indicating domain membership (1 = in domain, 0 = not)
        analysis_func: Analysis function to apply (svytotal, svymean, etc.)
        **kwargs: Additional arguments to pass to analysis_func

    Returns:
        DataFrame with analysis results for the domain
    """
    domain_condition = design.data[domain_var] == 1
    domain_design = design.subset(domain_condition)

    return analysis_func(domain_design, **kwargs)


def crosstab(
    design: MEPSSurveyDesign,
    row_var: str,
    col_var: str,
    statistic: str = "count",
) -> pd.DataFrame:
    """
    Create weighted cross-tabulation.

    Args:
        design: MEPSSurveyDesign object
        row_var: Row variable name
        col_var: Column variable name
        statistic: Statistic to compute ('count', 'proportion', 'row_pct', 'col_pct')

    Returns:
        DataFrame with cross-tabulation results
    """
    weights = design.get_weights()
    data = design.data.copy()
    data["_weight"] = weights

    pivot = data.pivot_table(
        values="_weight",
        index=row_var,
        columns=col_var,
        aggfunc="sum",
        fill_value=0,
    )

    if statistic == "proportion":
        pivot = pivot / pivot.values.sum()
    elif statistic == "row_pct":
        pivot = pivot.div(pivot.sum(axis=1), axis=0)
    elif statistic == "col_pct":
        pivot = pivot.div(pivot.sum(axis=0), axis=1)

    return pivot


def compare_groups(
    design: MEPSSurveyDesign,
    variable: str,
    group_var: str,
    test: str = "ttest",
) -> Dict[str, Any]:
    """
    Compare means between groups with survey weights.

    Args:
        design: MEPSSurveyDesign object
        variable: Variable to compare
        group_var: Grouping variable
        test: Type of test ('ttest', 'anova')

    Returns:
        Dictionary with test results
    """
    groups = design.data[group_var].unique()
    groups = [g for g in groups if pd.notna(g)]

    group_means = {}
    group_ses = {}

    for group in groups:
        group_condition = design.data[group_var] == group
        group_design = design.subset(group_condition)
        result = group_design.weighted_mean(variable)
        group_means[group] = result["estimate"]
        group_ses[group] = result["se"]

    if len(groups) == 2 and test == "ttest":
        g1, g2 = sorted(groups)[:2]
        diff = group_means[g1] - group_means[g2]
        se_diff = np.sqrt(group_ses[g1] ** 2 + group_ses[g2] ** 2)
        t_stat = diff / se_diff
        p_value = 2 * (1 - stats.t.cdf(abs(t_stat), df=len(design.data) - 2))

        return {
            "test": "t-test",
            "group_means": group_means,
            "group_ses": group_ses,
            "difference": diff,
            "se_difference": se_diff,
            "t_statistic": t_stat,
            "p_value": p_value,
        }

    return {
        "test": test,
        "group_means": group_means,
        "group_ses": group_ses,
    }
