"""
MEPS Survey Design Utilities

This module provides a wrapper class for survey-weighted analysis of MEPS data
using statsmodels. It implements functionality similar to R's survey package
and SAS SURVEY procedures.

IMPORTANT: MEPS data requires survey-weighted analysis to produce nationally
representative estimates with correct standard errors. All analyses must
account for the complex survey design using:
- VARPSU: Primary Sampling Units (clustering)
- VARSTR: Strata (stratification)
- PERWTyyF: Person weights (where yy is the 2-digit year)
"""

from typing import Optional, Union, List
import numpy as np
import pandas as pd
from scipy import stats


class MEPSSurveyDesign:
    """
    Survey design class for analyzing MEPS data with proper weighting.
    
    This class provides methods for calculating survey-weighted estimates
    including totals, means, proportions, and regression models.
    
    Parameters
    ----------
    data : pd.DataFrame
        DataFrame containing the MEPS data.
    strata : str, optional
        Name of the stratum variable. Default is 'VARSTR'.
    cluster : str, optional
        Name of the cluster/PSU variable. Default is 'VARPSU'.
    weights : str, optional
        Name of the weight variable. If not specified, will attempt to
        auto-detect based on year parameter.
    year : int, optional
        Data year (used for auto-detecting weight variable name).
    nest : bool, optional
        Whether PSUs are nested within strata. Default is True for MEPS.
        
    Examples
    --------
    >>> # Create survey design for 2018 data
    >>> design = MEPSSurveyDesign(data=fyc2018, year=2018)
    
    >>> # Or specify weight variable explicitly
    >>> design = MEPSSurveyDesign(data=fyc2018, weights='PERWT18F')
    
    >>> # Calculate weighted mean
    >>> result = design.mean('TOTEXP18')
    """
    
    def __init__(
        self,
        data: pd.DataFrame,
        strata: str = 'VARSTR',
        cluster: str = 'VARPSU',
        weights: Optional[str] = None,
        year: Optional[int] = None,
        nest: bool = True
    ):
        self.data = data.copy()
        self.strata = strata
        self.cluster = cluster
        self.nest = nest
        
        if weights is not None:
            self.weights = weights
        elif year is not None:
            self.weights = f'PERWT{year % 100:02d}F'
        else:
            weight_cols = [c for c in data.columns if c.startswith('PERWT') and c.endswith('F')]
            if weight_cols:
                self.weights = weight_cols[0]
            else:
                raise ValueError(
                    "Could not auto-detect weight variable. "
                    "Please specify 'weights' or 'year' parameter."
                )
        
        self._validate_design()
    
    def _validate_design(self):
        """Validate that required survey design variables exist."""
        missing = []
        for var in [self.strata, self.cluster, self.weights]:
            if var not in self.data.columns:
                missing.append(var)
        
        if missing:
            raise ValueError(f"Missing required survey design variables: {missing}")
    
    def subset(self, condition: Union[pd.Series, np.ndarray]) -> 'MEPSSurveyDesign':
        """
        Create a subset of the survey design for subpopulation analysis.
        
        IMPORTANT: For proper variance estimation, use this method instead of
        filtering the data directly. This maintains the full sample structure.
        
        Parameters
        ----------
        condition : pd.Series or np.ndarray
            Boolean array indicating which observations to include.
            
        Returns
        -------
        MEPSSurveyDesign
            New survey design object for the subpopulation.
        """
        new_design = MEPSSurveyDesign(
            data=self.data.copy(),
            strata=self.strata,
            cluster=self.cluster,
            weights=self.weights,
            nest=self.nest
        )
        new_design._subpop = condition
        return new_design
    
    def _get_weights(self) -> np.ndarray:
        """Get the weight values, applying subpopulation if defined."""
        weights = self.data[self.weights].values.astype(float)
        if hasattr(self, '_subpop'):
            weights = weights * self._subpop.astype(float)
        return weights
    
    def _get_data_subset(self) -> pd.DataFrame:
        """Get data subset if subpopulation is defined."""
        if hasattr(self, '_subpop'):
            return self.data[self._subpop]
        return self.data
    
    def total(
        self,
        variables: Union[str, List[str]],
        domain: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Calculate survey-weighted population totals.
        
        Parameters
        ----------
        variables : str or list of str
            Variable name(s) to calculate totals for.
        domain : str, optional
            Variable name for domain/subgroup analysis.
            
        Returns
        -------
        pd.DataFrame
            DataFrame with columns: variable, total, se, ci_lower, ci_upper
            
        Examples
        --------
        >>> # Total healthcare expenditures
        >>> design.total('TOTEXP18')
        
        >>> # Totals by insurance status
        >>> design.total('TOTEXP18', domain='INSCOV18')
        """
        if isinstance(variables, str):
            variables = [variables]
        
        results = []
        weights = self._get_weights()
        
        if domain is None:
            for var in variables:
                values = self.data[var].values.astype(float)
                total, se = self._calculate_total_with_se(values, weights)
                ci_lower, ci_upper = self._confidence_interval(total, se)
                results.append({
                    'variable': var,
                    'total': total,
                    'se': se,
                    'ci_lower': ci_lower,
                    'ci_upper': ci_upper
                })
        else:
            domain_values = self.data[domain].unique()
            for dval in sorted(domain_values):
                domain_mask = (self.data[domain] == dval).values
                for var in variables:
                    values = self.data[var].values.astype(float) * domain_mask
                    total, se = self._calculate_total_with_se(values, weights)
                    ci_lower, ci_upper = self._confidence_interval(total, se)
                    results.append({
                        'domain': domain,
                        'domain_value': dval,
                        'variable': var,
                        'total': total,
                        'se': se,
                        'ci_lower': ci_lower,
                        'ci_upper': ci_upper
                    })
        
        return pd.DataFrame(results)
    
    def mean(
        self,
        variables: Union[str, List[str]],
        domain: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Calculate survey-weighted means.
        
        Parameters
        ----------
        variables : str or list of str
            Variable name(s) to calculate means for.
        domain : str, optional
            Variable name for domain/subgroup analysis.
            
        Returns
        -------
        pd.DataFrame
            DataFrame with columns: variable, mean, se, ci_lower, ci_upper
            
        Examples
        --------
        >>> # Mean healthcare expenditures
        >>> design.mean('TOTEXP18')
        
        >>> # Mean by age group
        >>> design.mean('TOTEXP18', domain='AGECAT')
        """
        if isinstance(variables, str):
            variables = [variables]
        
        results = []
        weights = self._get_weights()
        
        if domain is None:
            for var in variables:
                values = self.data[var].values.astype(float)
                mean_val, se = self._calculate_mean_with_se(values, weights)
                ci_lower, ci_upper = self._confidence_interval(mean_val, se)
                results.append({
                    'variable': var,
                    'mean': mean_val,
                    'se': se,
                    'ci_lower': ci_lower,
                    'ci_upper': ci_upper
                })
        else:
            domain_values = self.data[domain].unique()
            for dval in sorted(domain_values):
                domain_mask = (self.data[domain] == dval).values
                domain_weights = weights * domain_mask
                for var in variables:
                    values = self.data[var].values.astype(float)
                    mean_val, se = self._calculate_mean_with_se(values, domain_weights)
                    ci_lower, ci_upper = self._confidence_interval(mean_val, se)
                    results.append({
                        'domain': domain,
                        'domain_value': dval,
                        'variable': var,
                        'mean': mean_val,
                        'se': se,
                        'ci_lower': ci_lower,
                        'ci_upper': ci_upper
                    })
        
        return pd.DataFrame(results)
    
    def proportion(
        self,
        variable: str,
        domain: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Calculate survey-weighted proportions for a categorical variable.
        
        Parameters
        ----------
        variable : str
            Categorical variable to calculate proportions for.
        domain : str, optional
            Variable name for domain/subgroup analysis.
            
        Returns
        -------
        pd.DataFrame
            DataFrame with proportions for each category.
        """
        results = []
        weights = self._get_weights()
        
        categories = self.data[variable].unique()
        
        if domain is None:
            total_weight = np.sum(weights)
            for cat in sorted(categories):
                cat_mask = (self.data[variable] == cat).values
                cat_weight = np.sum(weights * cat_mask)
                prop = cat_weight / total_weight if total_weight > 0 else 0
                se = self._calculate_proportion_se(cat_mask, weights)
                ci_lower, ci_upper = self._confidence_interval(prop, se)
                results.append({
                    'variable': variable,
                    'category': cat,
                    'proportion': prop,
                    'se': se,
                    'ci_lower': ci_lower,
                    'ci_upper': ci_upper
                })
        else:
            domain_values = self.data[domain].unique()
            for dval in sorted(domain_values):
                domain_mask = (self.data[domain] == dval).values
                domain_weights = weights * domain_mask
                total_domain_weight = np.sum(domain_weights)
                for cat in sorted(categories):
                    cat_mask = (self.data[variable] == cat).values
                    cat_weight = np.sum(domain_weights * cat_mask)
                    prop = cat_weight / total_domain_weight if total_domain_weight > 0 else 0
                    se = self._calculate_proportion_se(cat_mask, domain_weights)
                    ci_lower, ci_upper = self._confidence_interval(prop, se)
                    results.append({
                        'domain': domain,
                        'domain_value': dval,
                        'variable': variable,
                        'category': cat,
                        'proportion': prop,
                        'se': se,
                        'ci_lower': ci_lower,
                        'ci_upper': ci_upper
                    })
        
        return pd.DataFrame(results)
    
    def glm(
        self,
        formula: str,
        family: str = 'gaussian'
    ):
        """
        Fit a survey-weighted generalized linear model.
        
        Parameters
        ----------
        formula : str
            Model formula in patsy format (e.g., 'y ~ x1 + x2').
        family : str, optional
            Distribution family. Options: 'gaussian', 'binomial', 'poisson'.
            Default is 'gaussian'.
            
        Returns
        -------
        statsmodels GLM results object
            
        Examples
        --------
        >>> # Logistic regression for flu shot
        >>> result = design.glm('flu_shot ~ AGELAST + C(SEX) + C(RACETHX)', 
        ...                     family='binomial')
        >>> print(result.summary())
        """
        import statsmodels.api as sm
        import statsmodels.formula.api as smf
        
        family_map = {
            'gaussian': sm.families.Gaussian(),
            'binomial': sm.families.Binomial(),
            'poisson': sm.families.Poisson()
        }
        
        if family not in family_map:
            raise ValueError(f"Unknown family: {family}. Use one of: {list(family_map.keys())}")
        
        weights = self._get_weights()
        
        data_with_weights = self.data.copy()
        data_with_weights['_weights_'] = weights
        
        if hasattr(self, '_subpop'):
            data_with_weights = data_with_weights[self._subpop]
        
        model = smf.glm(
            formula=formula,
            data=data_with_weights,
            family=family_map[family],
            freq_weights=data_with_weights['_weights_']
        )
        
        return model.fit()
    
    def _calculate_total_with_se(
        self,
        values: np.ndarray,
        weights: np.ndarray
    ) -> tuple:
        """Calculate weighted total with standard error using Taylor linearization."""
        total = np.nansum(values * weights)
        
        strata = self.data[self.strata].values
        clusters = self.data[self.cluster].values
        
        unique_strata = np.unique(strata)
        variance = 0.0
        
        for stratum in unique_strata:
            stratum_mask = strata == stratum
            stratum_clusters = np.unique(clusters[stratum_mask])
            n_clusters = len(stratum_clusters)
            
            if n_clusters <= 1:
                continue
            
            cluster_totals = []
            for cluster in stratum_clusters:
                cluster_mask = stratum_mask & (clusters == cluster)
                cluster_total = np.nansum(values[cluster_mask] * weights[cluster_mask])
                cluster_totals.append(cluster_total)
            
            cluster_totals = np.array(cluster_totals)
            stratum_mean = np.mean(cluster_totals)
            
            stratum_var = n_clusters / (n_clusters - 1) * np.sum((cluster_totals - stratum_mean) ** 2)
            variance += stratum_var
        
        se = np.sqrt(variance)
        return total, se
    
    def _calculate_mean_with_se(
        self,
        values: np.ndarray,
        weights: np.ndarray
    ) -> tuple:
        """Calculate weighted mean with standard error."""
        valid_mask = ~np.isnan(values) & (weights > 0)
        valid_values = values[valid_mask]
        valid_weights = weights[valid_mask]
        
        if len(valid_values) == 0:
            return np.nan, np.nan
        
        total_weight = np.sum(valid_weights)
        mean_val = np.sum(valid_values * valid_weights) / total_weight
        
        residuals = valid_values - mean_val
        total, se_total = self._calculate_total_with_se(residuals, weights)
        se = se_total / total_weight
        
        return mean_val, se
    
    def _calculate_proportion_se(
        self,
        indicator: np.ndarray,
        weights: np.ndarray
    ) -> float:
        """Calculate standard error for a proportion."""
        valid_mask = weights > 0
        valid_indicator = indicator[valid_mask].astype(float)
        valid_weights = weights[valid_mask]
        
        if len(valid_indicator) == 0:
            return np.nan
        
        total_weight = np.sum(valid_weights)
        prop = np.sum(valid_indicator * valid_weights) / total_weight
        
        residuals = valid_indicator - prop
        _, se_total = self._calculate_total_with_se(
            residuals * valid_mask.astype(float),
            weights
        )
        se = se_total / total_weight
        
        return se
    
    def _confidence_interval(
        self,
        estimate: float,
        se: float,
        alpha: float = 0.05
    ) -> tuple:
        """Calculate confidence interval."""
        z = stats.norm.ppf(1 - alpha / 2)
        ci_lower = estimate - z * se
        ci_upper = estimate + z * se
        return ci_lower, ci_upper
    
    def describe(self) -> dict:
        """
        Get summary information about the survey design.
        
        Returns
        -------
        dict
            Dictionary with design information.
        """
        n_obs = len(self.data)
        n_strata = self.data[self.strata].nunique()
        n_clusters = self.data[self.cluster].nunique()
        total_weight = self.data[self.weights].sum()
        
        return {
            'n_observations': n_obs,
            'n_strata': n_strata,
            'n_clusters': n_clusters,
            'weight_variable': self.weights,
            'total_weight': total_weight,
            'strata_variable': self.strata,
            'cluster_variable': self.cluster
        }


def create_pooled_design(
    datasets: List[pd.DataFrame],
    years: List[int],
    linkage_file: Optional[pd.DataFrame] = None,
    pooled_strata: str = 'STRA9619',
    pooled_psu: str = 'PSU9619'
) -> MEPSSurveyDesign:
    """
    Create a survey design for pooled multi-year analysis.
    
    When pooling data across years (especially across the 2019 boundary),
    special variance estimation is required using the Pooled Variance Linkage file.
    
    Parameters
    ----------
    datasets : list of pd.DataFrame
        List of DataFrames to pool.
    years : list of int
        List of years corresponding to each dataset.
    linkage_file : pd.DataFrame, optional
        Pooled Variance Linkage file (required for 2017-2019+ pooling).
    pooled_strata : str, optional
        Name of pooled strata variable in linkage file.
    pooled_psu : str, optional
        Name of pooled PSU variable in linkage file.
        
    Returns
    -------
    MEPSSurveyDesign
        Survey design object for pooled analysis.
    """
    n_years = len(years)
    
    pooled_dfs = []
    for df, year in zip(datasets, years):
        df_copy = df.copy()
        weight_var = f'PERWT{year % 100:02d}F'
        df_copy['poolwt'] = df_copy[weight_var] / n_years
        df_copy['year'] = year
        pooled_dfs.append(df_copy)
    
    pooled = pd.concat(pooled_dfs, ignore_index=True)
    
    if linkage_file is not None:
        pooled = pooled.merge(
            linkage_file[['DUPERSID', 'PANEL', pooled_strata, pooled_psu]],
            on=['DUPERSID', 'PANEL'],
            how='left'
        )
        return MEPSSurveyDesign(
            data=pooled,
            strata=pooled_strata,
            cluster=pooled_psu,
            weights='poolwt'
        )
    else:
        return MEPSSurveyDesign(
            data=pooled,
            weights='poolwt'
        )
