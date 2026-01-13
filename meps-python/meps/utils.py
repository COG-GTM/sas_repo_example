"""
MEPS Utilities Module

Utility functions for data manipulation and variable creation.
Equivalent to R's dplyr functions and common data transformation patterns.
"""

from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd


def recode_factor(
    series: pd.Series,
    mapping: Dict[Any, str],
    default: str = "Missing",
    missing: str = "Missing",
) -> pd.Series:
    """
    Recode values to factor labels.
    Equivalent to R's dplyr::recode_factor().

    Args:
        series: pandas Series to recode
        mapping: Dictionary mapping original values to new labels
        default: Default value for unmapped values
        missing: Value for NA/missing values

    Returns:
        Recoded pandas Series as categorical

    Example:
        >>> poverty = recode_factor(
        ...     df['POVCAT17'],
        ...     {1: "Negative or poor", 2: "Near-poor", 3: "Low income",
        ...      4: "Middle income", 5: "High income"}
        ... )
    """
    result = series.map(mapping)
    result = result.fillna(default)
    result = result.where(series.notna(), missing)
    return pd.Categorical(result)


def create_indicator(
    df: pd.DataFrame,
    condition: pd.Series,
    name: Optional[str] = None,
) -> pd.Series:
    """
    Create a 0/1 indicator variable from a boolean condition.

    Args:
        df: DataFrame (for context)
        condition: Boolean Series
        name: Optional name for the resulting Series

    Returns:
        Integer Series with 0/1 values
    """
    result = condition.astype(int)
    if name:
        result.name = name
    return result


def standardize_variable_names(
    df: pd.DataFrame,
    year: int,
    variables: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Standardize year-specific variable names for pooling.
    Renames variables like PERWT18F, TOTEXP18 to perwt, totexp.

    Args:
        df: DataFrame with MEPS data
        year: Data year (used to identify year-specific suffixes)
        variables: Optional list of variable patterns to standardize

    Returns:
        DataFrame with standardized variable names

    Example:
        >>> fyc18 = standardize_variable_names(fyc18, 2018)
        >>> # PERWT18F -> perwt, TOTEXP18 -> totexp
    """
    df = df.copy()
    year_suffix = str(year)[-2:]

    rename_patterns = {
        f"PERWT{year_suffix}F": "perwt",
        f"TOTEXP{year_suffix}": "totexp",
        f"TOTSLF{year_suffix}": "totslf",
        f"INSCOV{year_suffix}": "inscov",
        f"SAQWT{year_suffix}F": "saqwt",
    }

    for old_name, new_name in rename_patterns.items():
        if old_name in df.columns:
            df = df.rename(columns={old_name: new_name})

    return df


def pool_weights(
    df: pd.DataFrame,
    weight_var: str,
    n_years: int,
) -> pd.DataFrame:
    """
    Create pooled weight variable by dividing by number of years.

    Args:
        df: DataFrame with MEPS data
        weight_var: Name of the weight variable
        n_years: Number of years being pooled

    Returns:
        DataFrame with new 'poolwt' column

    Example:
        >>> pooled = pool_weights(pooled, 'perwt', 3)
    """
    df = df.copy()
    df["poolwt"] = df[weight_var] / n_years
    return df


def merge_condition_event(
    conditions: pd.DataFrame,
    clnk: pd.DataFrame,
    events: pd.DataFrame,
    condition_filter: Optional[pd.Series] = None,
    event_type: Optional[int] = None,
) -> pd.DataFrame:
    """
    Merge conditions with events via the CLNK file.
    Handles de-duplication to prevent double-counting.

    Args:
        conditions: Conditions file DataFrame
        clnk: CLNK (Condition-Event Link) file DataFrame
        events: Events file DataFrame
        condition_filter: Optional boolean Series to filter conditions
        event_type: Optional event type to filter (1=OB, 2=OP, 3=ER, etc.)

    Returns:
        Merged DataFrame with conditions linked to events

    Example:
        >>> mental_health = merge_condition_event(
        ...     cond, clnk, ob,
        ...     condition_filter=cond['CCSR1X'].str.startswith('MBD'),
        ...     event_type=1
        ... )
    """
    if condition_filter is not None:
        conditions = conditions[condition_filter].copy()

    if event_type is not None:
        clnk = clnk[clnk["EVENTYPE"] == event_type].copy()

    merged = pd.merge(
        conditions,
        clnk,
        on=["DUPERSID", "CONDIDX"],
        how="inner",
    )

    merged = merged.drop_duplicates(subset=["EVNTIDX"])

    merged = pd.merge(
        merged,
        events,
        on=["DUPERSID", "EVNTIDX"],
        how="inner",
    )

    return merged


def aggregate_to_person(
    df: pd.DataFrame,
    group_vars: List[str],
    sum_vars: Optional[List[str]] = None,
    count_var: Optional[str] = None,
) -> pd.DataFrame:
    """
    Aggregate event-level data to person-level.

    Args:
        df: Event-level DataFrame
        group_vars: Variables to group by (typically includes DUPERSID)
        sum_vars: Variables to sum
        count_var: Name for the count variable

    Returns:
        Person-level aggregated DataFrame

    Example:
        >>> person_level = aggregate_to_person(
        ...     events,
        ...     group_vars=['DUPERSID', 'VARSTR', 'VARPSU', 'PERWT20F'],
        ...     sum_vars=['OBXP20X'],
        ...     count_var='n_visits'
        ... )
    """
    agg_dict = {}

    if sum_vars:
        for var in sum_vars:
            agg_dict[var] = "sum"

    if count_var:
        df = df.copy()
        df["_count"] = 1
        agg_dict["_count"] = "sum"

    result = df.groupby(group_vars, as_index=False).agg(agg_dict)

    if count_var:
        result = result.rename(columns={"_count": count_var})

    return result


def filter_ccsr(
    df: pd.DataFrame,
    ccsr_codes: Union[str, List[str]],
    ccsr_vars: List[str] = ["CCSR1X", "CCSR2X", "CCSR3X"],
) -> pd.DataFrame:
    """
    Filter conditions by CCSR code(s).

    Args:
        df: Conditions DataFrame
        ccsr_codes: CCSR code(s) to filter for (e.g., 'END010' for hyperlipidemia)
        ccsr_vars: CCSR variable names to check

    Returns:
        Filtered DataFrame

    Example:
        >>> hyperlipidemia = filter_ccsr(conditions, 'END010')
        >>> mental_health = filter_ccsr(conditions, ['MBD001', 'MBD002', 'MBD003'])
    """
    if isinstance(ccsr_codes, str):
        ccsr_codes = [ccsr_codes]

    mask = pd.Series(False, index=df.index)
    for var in ccsr_vars:
        if var in df.columns:
            mask |= df[var].isin(ccsr_codes)

    return df[mask].copy()


def filter_ccsr_pattern(
    df: pd.DataFrame,
    pattern: str,
    ccsr_vars: List[str] = ["CCSR1X", "CCSR2X", "CCSR3X"],
) -> pd.DataFrame:
    """
    Filter conditions by CCSR code pattern using regex.

    Args:
        df: Conditions DataFrame
        pattern: Regex pattern to match (e.g., 'MBD|FAC002|FAC007')
        ccsr_vars: CCSR variable names to check

    Returns:
        Filtered DataFrame

    Example:
        >>> mental_health = filter_ccsr_pattern(conditions, 'MBD|FAC002|FAC007|NVS011')
    """
    all_ccsr = df[ccsr_vars].fillna("").astype(str).agg("".join, axis=1)
    mask = all_ccsr.str.contains(pattern, regex=True)
    return df[mask].copy()


def create_subpop(
    df: pd.DataFrame,
    condition: pd.Series,
    name: str = "subpop",
) -> pd.DataFrame:
    """
    Create a subpopulation indicator variable.
    This is the proper way to subset for survey analysis.

    Args:
        df: DataFrame
        condition: Boolean condition for subpopulation
        name: Name for the subpopulation variable

    Returns:
        DataFrame with subpopulation indicator

    Example:
        >>> df = create_subpop(df, df['AGELAST'] >= 18, 'adults')
    """
    df = df.copy()
    df[name] = condition.astype(int)
    return df


def combine_years(
    dataframes: List[pd.DataFrame],
    year_var: str = "year",
    years: Optional[List[int]] = None,
) -> pd.DataFrame:
    """
    Combine multiple years of data for pooling.

    Args:
        dataframes: List of DataFrames to combine
        year_var: Name for the year variable
        years: Optional list of years corresponding to each DataFrame

    Returns:
        Combined DataFrame

    Example:
        >>> pooled = combine_years([fyc17, fyc18, fyc19], years=[2017, 2018, 2019])
    """
    if years is None:
        years = list(range(len(dataframes)))

    for i, (df, year) in enumerate(zip(dataframes, years)):
        dataframes[i] = df.copy()
        dataframes[i][year_var] = year

    return pd.concat(dataframes, ignore_index=True)


def format_currency(value: float, decimals: int = 0) -> str:
    """
    Format a number as currency.

    Args:
        value: Numeric value
        decimals: Number of decimal places

    Returns:
        Formatted string

    Example:
        >>> format_currency(1234567.89)
        '$1,234,568'
    """
    if decimals == 0:
        return f"${value:,.0f}"
    return f"${value:,.{decimals}f}"


def format_percent(value: float, decimals: int = 2) -> str:
    """
    Format a proportion as percentage.

    Args:
        value: Proportion (0-1)
        decimals: Number of decimal places

    Returns:
        Formatted string

    Example:
        >>> format_percent(0.1234)
        '12.34%'
    """
    return f"{value * 100:.{decimals}f}%"


def format_estimate(
    estimate: float,
    se: float,
    format_type: str = "number",
    decimals: int = 2,
) -> str:
    """
    Format an estimate with standard error.

    Args:
        estimate: Point estimate
        se: Standard error
        format_type: 'number', 'currency', or 'percent'
        decimals: Number of decimal places

    Returns:
        Formatted string

    Example:
        >>> format_estimate(1234567, 12345, 'currency')
        '$1,234,567 (SE: $12,345)'
    """
    if format_type == "currency":
        return f"{format_currency(estimate, decimals)} (SE: {format_currency(se, decimals)})"
    elif format_type == "percent":
        return f"{format_percent(estimate, decimals)} (SE: {format_percent(se, decimals)})"
    else:
        return f"{estimate:,.{decimals}f} (SE: {se:,.{decimals}f})"
