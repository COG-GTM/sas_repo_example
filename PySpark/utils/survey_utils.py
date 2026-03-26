"""
Survey-Weighted Estimation Utilities for MEPS PySpark Analysis

Provides wrapper functions for complex survey estimation using a hybrid approach:
PySpark handles data preparation (ETL), then data is collected to the driver
and Python survey libraries (samplics, statsmodels) perform the estimation.

Replaces SAS procedures:
  - PROC SURVEYMEANS  -> survey_mean(), survey_total()
  - PROC SURVEYFREQ   -> survey_freq()
  - PROC SURVEYLOGISTIC -> survey_logistic()
  - PROC SURVEYREG    -> survey_reg()
"""

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame

from samplics.estimation import TaylorEstimator


def survey_mean(
    spark_df: DataFrame,
    var_cols: list,
    stratum_col: str = "VARSTR",
    cluster_col: str = "VARPSU",
    weight_col: str = "PERWT20F",
    domain_col: str = None,
) -> pd.DataFrame:
    """Calculate survey-weighted means with standard errors.

    Equivalent of SAS:
        PROC SURVEYMEANS DATA=dataset MEAN STDERR;
            STRATA VARSTR; CLUSTER VARPSU; WEIGHT PERWTyyF;
            VAR var1 var2;
            DOMAIN domain_var;
        RUN;

    Args:
        spark_df: PySpark DataFrame with analysis variables.
        var_cols: List of column names to estimate means for.
        stratum_col: Stratification variable (default VARSTR).
        cluster_col: Cluster/PSU variable (default VARPSU).
        weight_col: Survey weight variable.
        domain_col: Optional domain/subpopulation variable for subgroup analysis.

    Returns:
        pandas DataFrame with columns: variable, domain (if applicable),
        mean, se, lower_ci, upper_ci, nobs, sum_weights.
    """
    needed = list(set(var_cols + [stratum_col, cluster_col, weight_col]
                      + ([domain_col] if domain_col else [])))
    pdf = spark_df.select(needed).toPandas()

    results = []
    for var in var_cols:
        estimator = TaylorEstimator("mean")
        y = pdf[var].astype(float).values
        w = pdf[weight_col].astype(float).values
        stratum = pdf[stratum_col].values
        psu = pdf[cluster_col].values

        if domain_col:
            domain = pdf[domain_col].values
            estimator.estimate(
                y=y, samp_weight=w, stratum=stratum, psu=psu, domain=domain
            )
        else:
            estimator.estimate(
                y=y, samp_weight=w, stratum=stratum, psu=psu
            )

        est_df = estimator.to_dataframe()
        est_df["variable"] = var
        results.append(est_df)

    return pd.concat(results, ignore_index=True)


def survey_total(
    spark_df: DataFrame,
    var_cols: list,
    stratum_col: str = "VARSTR",
    cluster_col: str = "VARPSU",
    weight_col: str = "PERWT20F",
    domain_col: str = None,
) -> pd.DataFrame:
    """Calculate survey-weighted totals (sums) with standard errors.

    Equivalent of SAS:
        PROC SURVEYMEANS DATA=dataset SUM STD;
            STRATA VARSTR; CLUSTER VARPSU; WEIGHT PERWTyyF;
            VAR var1 var2;
            DOMAIN domain_var;
        RUN;

    Args:
        spark_df: PySpark DataFrame with analysis variables.
        var_cols: List of column names to estimate totals for.
        stratum_col: Stratification variable.
        cluster_col: Cluster/PSU variable.
        weight_col: Survey weight variable.
        domain_col: Optional domain/subpopulation variable.

    Returns:
        pandas DataFrame with total estimates and standard errors.
    """
    needed = list(set(var_cols + [stratum_col, cluster_col, weight_col]
                      + ([domain_col] if domain_col else [])))
    pdf = spark_df.select(needed).toPandas()

    results = []
    for var in var_cols:
        estimator = TaylorEstimator("total")
        y = pdf[var].astype(float).values
        w = pdf[weight_col].astype(float).values
        stratum = pdf[stratum_col].values
        psu = pdf[cluster_col].values

        if domain_col:
            domain = pdf[domain_col].values
            estimator.estimate(
                y=y, samp_weight=w, stratum=stratum, psu=psu, domain=domain
            )
        else:
            estimator.estimate(
                y=y, samp_weight=w, stratum=stratum, psu=psu
            )

        est_df = estimator.to_dataframe()
        est_df["variable"] = var
        results.append(est_df)

    return pd.concat(results, ignore_index=True)


def survey_freq(
    spark_df: DataFrame,
    table_vars: list,
    stratum_col: str = "VARSTR",
    cluster_col: str = "VARPSU",
    weight_col: str = "PERWT20F",
    domain_col: str = None,
) -> pd.DataFrame:
    """Calculate survey-weighted frequency tables with standard errors.

    Equivalent of SAS:
        PROC SURVEYFREQ DATA=dataset;
            STRATA VARSTR; CLUSTER VARPSU; WEIGHT PERWTyyF;
            TABLES var1 * var2 / ROW COL;
        RUN;

    Args:
        spark_df: PySpark DataFrame.
        table_vars: List of variable names for the frequency table.
        stratum_col: Stratification variable.
        cluster_col: Cluster/PSU variable.
        weight_col: Survey weight variable.
        domain_col: Optional domain variable.

    Returns:
        pandas DataFrame with frequency estimates and proportions.
    """
    needed = list(set(table_vars + [stratum_col, cluster_col, weight_col]
                      + ([domain_col] if domain_col else [])))
    pdf = spark_df.select(needed).toPandas()

    estimator = TaylorEstimator("proportion")

    if len(table_vars) == 1:
        y = pdf[table_vars[0]].values
    else:
        # For cross-tabulations, create a combined variable
        pdf["_combined_"] = ""
        for v in table_vars:
            pdf["_combined_"] = pdf["_combined_"] + pdf[v].astype(str) + "_"
        y = pdf["_combined_"].values

    w = pdf[weight_col].astype(float).values
    stratum = pdf[stratum_col].values
    psu = pdf[cluster_col].values

    if domain_col:
        domain = pdf[domain_col].values
        estimator.estimate(
            y=y, samp_weight=w, stratum=stratum, psu=psu, domain=domain
        )
    else:
        estimator.estimate(
            y=y, samp_weight=w, stratum=stratum, psu=psu
        )

    return estimator.to_dataframe()


def survey_logistic(
    spark_df: DataFrame,
    outcome_col: str,
    predictor_cols: list,
    stratum_col: str = "VARSTR",
    cluster_col: str = "VARPSU",
    weight_col: str = "PERWT20F",
    class_vars: dict = None,
) -> object:
    """Fit a survey-weighted logistic regression model.

    Equivalent of SAS:
        PROC SURVEYLOGISTIC DATA=dataset;
            STRATA VARSTR; CLUSTER VARPSU; WEIGHT PERWTyyF;
            CLASS sex (ref='Male') racethx (ref='Hispanic') / param=ref;
            MODEL outcome(ref='0') = age sex racethx;
        RUN;

    Args:
        spark_df: PySpark DataFrame.
        outcome_col: Binary outcome variable name.
        predictor_cols: List of predictor variable names.
        stratum_col: Stratification variable.
        cluster_col: Cluster/PSU variable.
        weight_col: Survey weight variable.
        class_vars: Dict of {var_name: reference_level} for categorical predictors.
            If None, all predictors are treated as continuous.

    Returns:
        Fitted statsmodels GLM result object with survey weights.
    """
    import statsmodels.api as sm

    needed = list(set(
        [outcome_col] + predictor_cols
        + [stratum_col, cluster_col, weight_col]
    ))
    pdf = spark_df.select(needed).toPandas().dropna(
        subset=[outcome_col] + predictor_cols + [weight_col]
    )

    # Create design matrix with dummy variables for class vars
    X_parts = []
    for col in predictor_cols:
        if class_vars and col in class_vars:
            ref = class_vars[col]
            dummies = pd.get_dummies(pdf[col], prefix=col, drop_first=False)
            # Drop the reference level column
            ref_col_name = f"{col}_{ref}"
            dummies = dummies.drop(columns=[ref_col_name], errors="ignore")
            X_parts.append(dummies)
        else:
            X_parts.append(pdf[[col]].astype(float))

    X = pd.concat(X_parts, axis=1)
    X = sm.add_constant(X)
    y = pdf[outcome_col].astype(float)
    weights = pdf[weight_col].astype(float)

    model = sm.GLM(y, X, family=sm.families.Binomial(), freq_weights=weights)
    result = model.fit()

    print(result.summary())
    return result


def survey_reg(
    spark_df: DataFrame,
    outcome_col: str,
    predictor_cols: list,
    stratum_col: str = "VARSTR",
    cluster_col: str = "VARPSU",
    weight_col: str = "PERWT20F",
) -> object:
    """Fit a survey-weighted linear regression model.

    Equivalent of SAS:
        PROC SURVEYREG DATA=dataset;
            STRATA VARSTR; CLUSTER VARPSU; WEIGHT PERWTyyF;
            MODEL outcome = predictor1 predictor2;
        RUN;

    Args:
        spark_df: PySpark DataFrame.
        outcome_col: Continuous outcome variable name.
        predictor_cols: List of predictor variable names.
        stratum_col: Stratification variable.
        cluster_col: Cluster/PSU variable.
        weight_col: Survey weight variable.

    Returns:
        Fitted statsmodels WLS result object.
    """
    import statsmodels.api as sm

    needed = list(set(
        [outcome_col] + predictor_cols
        + [stratum_col, cluster_col, weight_col]
    ))
    pdf = spark_df.select(needed).toPandas().dropna(
        subset=[outcome_col] + predictor_cols + [weight_col]
    )

    X = pdf[predictor_cols].astype(float)
    X = sm.add_constant(X)
    y = pdf[outcome_col].astype(float)
    weights = pdf[weight_col].astype(float)

    model = sm.WLS(y, X, weights=weights)
    result = model.fit()

    print(result.summary())
    return result
