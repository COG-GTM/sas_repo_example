"""
Variance estimation utilities for complex survey designs.

This module provides functions for calculating variance estimates
for survey statistics using Taylor series linearization, which is
the method used by SAS PROC SURVEYMEANS.

For MEPS data, the survey design includes:
- Stratification (VARSTR)
- Clustering/PSUs (VARPSU)
- Person weights (PERWT16F)
"""

from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def calculate_psu_statistics(
    df: DataFrame,
    stratum_var: str = "VARSTR",
    psu_var: str = "VARPSU",
    weight_var: str = "PERWT16F",
    analysis_vars: Optional[List[str]] = None,
    group_vars: Optional[List[str]] = None,
) -> DataFrame:
    """
    Calculate PSU-level statistics for variance estimation.

    Args:
        df (DataFrame): Input DataFrame with survey data
        stratum_var (str): Name of stratification variable
        psu_var (str): Name of PSU variable
        weight_var (str): Name of weight variable
        analysis_vars (List[str], optional): Variables to analyze
        group_vars (List[str], optional): Additional grouping variables

    Returns:
        DataFrame: PSU-level statistics
    """
    if analysis_vars is None:
        analysis_vars = ["TOTAL"]

    group_cols = [stratum_var, psu_var]
    if group_vars:
        group_cols.extend(group_vars)

    agg_exprs = [
        F.sum(weight_var).alias("psu_weight"),
        F.count("*").alias("psu_sample_size"),
    ]

    for var in analysis_vars:
        agg_exprs.append(
            F.sum(F.col(var) * F.col(weight_var)).alias(f"psu_weighted_{var}")
        )
        agg_exprs.append(F.sum(var).alias(f"psu_sum_{var}"))
        agg_exprs.append(F.mean(var).alias(f"psu_mean_{var}"))

    psu_stats = df.groupBy(*group_cols).agg(*agg_exprs)

    return psu_stats


def calculate_stratum_statistics(
    psu_df: DataFrame,
    stratum_var: str = "VARSTR",
    psu_var: str = "VARPSU",
    group_vars: Optional[List[str]] = None,
) -> DataFrame:
    """
    Calculate stratum-level statistics from PSU statistics.

    This function aggregates PSU-level statistics to the stratum level
    and calculates variance components.

    Args:
        psu_df (DataFrame): PSU-level statistics
        stratum_var (str): Name of stratification variable
        psu_var (str): Name of PSU variable
        group_vars (List[str], optional): Additional grouping variables

    Returns:
        DataFrame: Stratum-level variance components
    """
    group_cols = [stratum_var]
    if group_vars:
        group_cols.extend(group_vars)

    weighted_cols = [col for col in psu_df.columns if col.startswith("psu_weighted_")]

    agg_exprs = [
        F.count(psu_var).alias("n_psu"),
        F.sum("psu_weight").alias("stratum_weight"),
        F.sum("psu_sample_size").alias("stratum_sample_size"),
    ]

    for col in weighted_cols:
        var_name = col.replace("psu_weighted_", "")
        agg_exprs.append(F.mean(col).alias(f"mean_psu_weighted_{var_name}"))
        agg_exprs.append(F.variance(col).alias(f"var_psu_weighted_{var_name}"))
        agg_exprs.append(F.stddev(col).alias(f"sd_psu_weighted_{var_name}"))

    stratum_stats = psu_df.groupBy(*group_cols).agg(*agg_exprs)

    for col in weighted_cols:
        var_name = col.replace("psu_weighted_", "")
        stratum_stats = stratum_stats.withColumn(
            f"se_{var_name}",
            F.sqrt(
                F.col(f"var_psu_weighted_{var_name}")
                * F.col("n_psu")
                / (F.col("n_psu") - 1)
            ),
        )

    return stratum_stats


def calculate_taylor_series_variance(
    df: DataFrame,
    stratum_var: str = "VARSTR",
    psu_var: str = "VARPSU",
    weight_var: str = "PERWT16F",
    analysis_var: str = "TOTAL",
    group_vars: Optional[List[str]] = None,
) -> DataFrame:
    """
    Calculate variance using Taylor series linearization.

    This is the primary variance estimation method used by SAS PROC SURVEYMEANS.

    Args:
        df (DataFrame): Input DataFrame with survey data
        stratum_var (str): Name of stratification variable
        psu_var (str): Name of PSU variable
        weight_var (str): Name of weight variable
        analysis_var (str): Variable to analyze
        group_vars (List[str], optional): Additional grouping variables

    Returns:
        DataFrame: Variance estimates
    """
    psu_stats = calculate_psu_statistics(
        df,
        stratum_var=stratum_var,
        psu_var=psu_var,
        weight_var=weight_var,
        analysis_vars=[analysis_var],
        group_vars=group_vars,
    )

    stratum_stats = calculate_stratum_statistics(
        psu_stats,
        stratum_var=stratum_var,
        psu_var=psu_var,
        group_vars=group_vars,
    )

    return stratum_stats


def combine_stratum_variances(
    stratum_df: DataFrame, group_vars: Optional[List[str]] = None
) -> DataFrame:
    """
    Combine variance estimates across all strata.

    Args:
        stratum_df (DataFrame): Stratum-level variance components
        group_vars (List[str], optional): Grouping variables

    Returns:
        DataFrame: Combined variance estimates
    """
    if group_vars is None:
        group_vars = []

    se_cols = [col for col in stratum_df.columns if col.startswith("se_")]

    agg_exprs = [
        F.sum("stratum_weight").alias("total_weight"),
        F.sum("n_psu").alias("total_psu"),
        F.sum("stratum_sample_size").alias("total_sample_size"),
    ]

    for se_col in se_cols:
        var_name = se_col.replace("se_", "")
        agg_exprs.append(
            F.sum(F.pow(F.col(se_col), 2)).alias(f"total_variance_{var_name}")
        )

    if group_vars:
        combined = stratum_df.groupBy(*group_vars).agg(*agg_exprs)
    else:
        combined = stratum_df.agg(*agg_exprs)

    for se_col in se_cols:
        var_name = se_col.replace("se_", "")
        combined = combined.withColumn(
            f"combined_se_{var_name}", F.sqrt(F.col(f"total_variance_{var_name}"))
        )

    return combined


def calculate_coefficient_of_variation(
    df: DataFrame, mean_col: str, se_col: str, cv_col_name: str = "cv"
) -> DataFrame:
    """
    Calculate coefficient of variation (CV).

    CV = (Standard Error / Mean) * 100

    Args:
        df (DataFrame): Input DataFrame
        mean_col (str): Name of mean column
        se_col (str): Name of standard error column
        cv_col_name (str): Name for output CV column

    Returns:
        DataFrame: DataFrame with CV added
    """
    df = df.withColumn(
        cv_col_name,
        F.when(F.col(mean_col) != 0, (F.col(se_col) / F.col(mean_col)) * 100).otherwise(
            None
        ),
    )

    return df


def calculate_confidence_interval(
    df: DataFrame,
    estimate_col: str,
    se_col: str,
    confidence_level: float = 0.95,
    ci_lower_col: str = "ci_lower",
    ci_upper_col: str = "ci_upper",
) -> DataFrame:
    """
    Calculate confidence intervals for estimates.

    Uses normal approximation: estimate ± z * SE

    Args:
        df (DataFrame): Input DataFrame
        estimate_col (str): Name of estimate column
        se_col (str): Name of standard error column
        confidence_level (float): Confidence level (default 0.95 for 95% CI)
        ci_lower_col (str): Name for lower CI bound column
        ci_upper_col (str): Name for upper CI bound column

    Returns:
        DataFrame: DataFrame with confidence intervals added
    """
    from scipy import stats

    z_score = stats.norm.ppf((1 + confidence_level) / 2)

    df = df.withColumn(ci_lower_col, F.col(estimate_col) - (z_score * F.col(se_col)))

    df = df.withColumn(ci_upper_col, F.col(estimate_col) + (z_score * F.col(se_col)))

    return df


def calculate_design_effect(
    complex_survey_variance: float, simple_random_variance: float
) -> float:
    """
    Calculate design effect (DEFF).

    DEFF = Variance under complex design / Variance under SRS

    Args:
        complex_survey_variance (float): Variance from complex survey design
        simple_random_variance (float): Variance under simple random sampling

    Returns:
        float: Design effect
    """
    if simple_random_variance == 0:
        return None
    return complex_survey_variance / simple_random_variance


def calculate_effective_sample_size(
    actual_sample_size: int, design_effect: float
) -> float:
    """
    Calculate effective sample size.

    n_eff = n / DEFF

    Args:
        actual_sample_size (int): Actual sample size
        design_effect (float): Design effect

    Returns:
        float: Effective sample size
    """
    if design_effect == 0:
        return None
    return actual_sample_size / design_effect
