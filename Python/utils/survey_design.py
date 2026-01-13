"""
MEPS Survey Design

This module provides a wrapper class for survey-weighted analysis of MEPS data
using statsmodels. The complex survey design of MEPS requires special methods
to ensure unbiased estimates and proper standard errors.

MEPS uses a stratified, clustered sample design with the following key variables:
- VARPSU: Primary Sampling Units (clusters)
- VARSTR: Strata
- PERWTyyF: Person-level weights (where yy is the 2-digit year)

All analyses should use these survey design variables to produce nationally
representative estimates with correct standard errors.
"""

from typing import Optional, Union, List, Dict, Any
import warnings

import numpy as np
import pandas as pd


class MEPSSurveyDesign:
    """
    Survey design wrapper for MEPS data analysis.

    This class provides methods for survey-weighted analysis of MEPS data,
    including totals, means, proportions, and regression models.

    Parameters
    ----------
    data : pd.DataFrame
        The MEPS data to analyze.
    year : int
        The data year (used to determine the weight variable name).
    strata : str, optional
        The stratification variable. Default is 'VARSTR'.
    cluster : str, optional
        The cluster (PSU) variable. Default is 'VARPSU'.
    weights : str, optional
        The weight variable. If None, defaults to 'PERWTyyF' where yy is the
        2-digit year. Can also specify 'SAQWT' for SAQ weights.
    nest : bool, optional
        Whether PSUs are nested within strata. Default is True for MEPS data.

    Examples
    --------
    >>> import pandas as pd
    >>> from utils.meps_loader import load_meps_data
    >>> from utils.survey_design import MEPSSurveyDesign
    >>>
    >>> # Load 2020 FYC data
    >>> data = load_meps_data("C:/MEPS/h224.dta")
    >>>
    >>> # Create survey design
    >>> design = MEPSSurveyDesign(data, year=2020)
    >>>
    >>> # Calculate total expenditures
    >>> results = design.total('TOTEXP20')
    >>> print(results)
    """

    def __init__(
        self,
        data: pd.DataFrame,
        year: int,
        strata: str = 'VARSTR',
        cluster: str = 'VARPSU',
        weights: Optional[str] = None,
        nest: bool = True
    ):
        self.data = data.copy()
        self.year = year
        self.strata = strata
        self.cluster = cluster
        self.nest = nest

        if weights is None:
            self.weights = f'PERWT{year % 100:02d}F'
        else:
            if weights == 'SAQWT':
                self.weights = f'SAQWT{year % 100:02d}F'
            else:
                self.weights = weights

        self._validate_design()

    def _validate_design(self):
        """Validate that required survey design variables exist."""
        required_vars = [self.strata, self.cluster, self.weights]
        missing_vars = [v for v in required_vars if v not in self.data.columns]

        if missing_vars:
            raise ValueError(
                f"Missing required survey design variables: {missing_vars}. "
                f"Available columns: {list(self.data.columns)[:20]}..."
            )

        if self.data[self.weights].isna().all():
            warnings.warn(
                f"All values in weight variable '{self.weights}' are missing."
            )

    def _get_subset(
        self,
        domain: Optional[str] = None,
        domain_value: Optional[Any] = None
    ) -> pd.DataFrame:
        """Get subset of data for domain analysis."""
        if domain is None:
            return self.data

        if domain_value is not None:
            mask = self.data[domain] == domain_value
        else:
            mask = self.data[domain].astype(bool)

        return self.data[mask]

    def total(
        self,
        variables: Union[str, List[str]],
        domain: Optional[str] = None,
        domain_value: Optional[Any] = None
    ) -> pd.DataFrame:
        """
        Calculate survey-weighted population totals.

        Parameters
        ----------
        variables : str or list of str
            Variable(s) to calculate totals for.
        domain : str, optional
            Domain variable for subpopulation analysis.
        domain_value : any, optional
            Specific value of domain variable to analyze.

        Returns
        -------
        pd.DataFrame
            DataFrame with columns: Variable, Total, SE, N, SumWgt

        Examples
        --------
        >>> # Total expenditures
        >>> design.total('TOTEXP20')

        >>> # Total by domain
        >>> design.total('TOTEXP20', domain='AGECAT', domain_value=1)
        """
        if isinstance(variables, str):
            variables = [variables]

        subset = self._get_subset(domain, domain_value)

        results = []
        for var in variables:
            if var not in subset.columns:
                warnings.warn(f"Variable '{var}' not found in data.")
                continue

            valid_mask = subset[var].notna() & subset[self.weights].notna()
            valid_data = subset[valid_mask]

            total = (valid_data[var] * valid_data[self.weights]).sum()

            se = self._calculate_se_total(valid_data, var)

            results.append({
                'Variable': var,
                'Total': total,
                'SE': se,
                'N': len(valid_data),
                'SumWgt': valid_data[self.weights].sum()
            })

        return pd.DataFrame(results)

    def mean(
        self,
        variables: Union[str, List[str]],
        domain: Optional[str] = None,
        domain_value: Optional[Any] = None
    ) -> pd.DataFrame:
        """
        Calculate survey-weighted means.

        Parameters
        ----------
        variables : str or list of str
            Variable(s) to calculate means for.
        domain : str, optional
            Domain variable for subpopulation analysis.
        domain_value : any, optional
            Specific value of domain variable to analyze.

        Returns
        -------
        pd.DataFrame
            DataFrame with columns: Variable, Mean, SE, N, SumWgt

        Examples
        --------
        >>> # Mean expenditures
        >>> design.mean('TOTEXP20')

        >>> # Mean by domain (e.g., for persons with expense)
        >>> design.mean('TOTEXP20', domain='has_expense', domain_value=1)
        """
        if isinstance(variables, str):
            variables = [variables]

        subset = self._get_subset(domain, domain_value)

        results = []
        for var in variables:
            if var not in subset.columns:
                warnings.warn(f"Variable '{var}' not found in data.")
                continue

            valid_mask = subset[var].notna() & subset[self.weights].notna()
            valid_data = subset[valid_mask]

            weighted_sum = (valid_data[var] * valid_data[self.weights]).sum()
            sum_weights = valid_data[self.weights].sum()
            mean_val = weighted_sum / sum_weights if sum_weights > 0 else np.nan

            se = self._calculate_se_mean(valid_data, var)

            results.append({
                'Variable': var,
                'Mean': mean_val,
                'SE': se,
                'N': len(valid_data),
                'SumWgt': sum_weights
            })

        return pd.DataFrame(results)

    def proportion(
        self,
        variables: Union[str, List[str]],
        domain: Optional[str] = None,
        domain_value: Optional[Any] = None
    ) -> pd.DataFrame:
        """
        Calculate survey-weighted proportions for binary (0/1) variables.

        This is equivalent to calculating the mean of a binary variable.

        Parameters
        ----------
        variables : str or list of str
            Binary variable(s) to calculate proportions for.
        domain : str, optional
            Domain variable for subpopulation analysis.
        domain_value : any, optional
            Specific value of domain variable to analyze.

        Returns
        -------
        pd.DataFrame
            DataFrame with columns: Variable, Proportion, SE, N, SumWgt
        """
        result = self.mean(variables, domain, domain_value)
        result = result.rename(columns={'Mean': 'Proportion'})
        return result

    def group_by(
        self,
        variables: Union[str, List[str]],
        by: str,
        stat: str = 'mean'
    ) -> pd.DataFrame:
        """
        Calculate survey-weighted statistics by group.

        Parameters
        ----------
        variables : str or list of str
            Variable(s) to analyze.
        by : str
            Grouping variable.
        stat : str, optional
            Statistic to calculate: 'mean', 'total', or 'proportion'.
            Default is 'mean'.

        Returns
        -------
        pd.DataFrame
            DataFrame with statistics for each group.

        Examples
        --------
        >>> # Mean expenditures by age group
        >>> design.group_by('TOTEXP20', by='AGECAT', stat='mean')

        >>> # Total expenditures by insurance status
        >>> design.group_by('TOTEXP20', by='INSCOV20', stat='total')
        """
        if isinstance(variables, str):
            variables = [variables]

        if by not in self.data.columns:
            raise ValueError(f"Grouping variable '{by}' not found in data.")

        groups = self.data[by].dropna().unique()

        all_results = []
        for group_val in sorted(groups):
            if stat == 'mean':
                result = self.mean(variables, domain=by, domain_value=group_val)
            elif stat == 'total':
                result = self.total(variables, domain=by, domain_value=group_val)
            elif stat == 'proportion':
                result = self.proportion(variables, domain=by, domain_value=group_val)
            else:
                raise ValueError(f"Unknown stat: {stat}. Use 'mean', 'total', or 'proportion'.")

            result[by] = group_val
            all_results.append(result)

        return pd.concat(all_results, ignore_index=True)

    def _calculate_se_total(self, data: pd.DataFrame, var: str) -> float:
        """
        Calculate standard error for total using Taylor series linearization.

        This is a simplified implementation. For production use, consider
        using statsmodels or other specialized survey packages.
        """
        strata_groups = data.groupby(self.strata)
        n_strata = len(strata_groups)

        if n_strata == 0:
            return np.nan

        variance = 0.0
        for _, stratum_data in strata_groups:
            psu_groups = stratum_data.groupby(self.cluster)
            n_psu = len(psu_groups)

            if n_psu <= 1:
                continue

            psu_totals = []
            for _, psu_data in psu_groups:
                psu_total = (psu_data[var] * psu_data[self.weights]).sum()
                psu_totals.append(psu_total)

            psu_totals = np.array(psu_totals)
            stratum_mean = psu_totals.mean()
            stratum_var = n_psu / (n_psu - 1) * np.sum((psu_totals - stratum_mean) ** 2)
            variance += stratum_var

        return np.sqrt(variance)

    def _calculate_se_mean(self, data: pd.DataFrame, var: str) -> float:
        """
        Calculate standard error for mean using Taylor series linearization.

        This is a simplified implementation. For production use, consider
        using statsmodels or other specialized survey packages.
        """
        total_weight = data[self.weights].sum()
        if total_weight == 0:
            return np.nan

        weighted_mean = (data[var] * data[self.weights]).sum() / total_weight

        data = data.copy()
        data['_resid'] = data[var] - weighted_mean

        se_total = self._calculate_se_total(data, '_resid')

        return se_total / total_weight

    def glm(
        self,
        formula: str,
        family: str = 'gaussian',
        domain: Optional[str] = None,
        domain_value: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        Fit a survey-weighted generalized linear model.

        Parameters
        ----------
        formula : str
            Model formula in patsy format (e.g., 'y ~ x1 + x2').
        family : str, optional
            Distribution family: 'gaussian', 'binomial', 'poisson'.
            Default is 'gaussian'.
        domain : str, optional
            Domain variable for subpopulation analysis.
        domain_value : any, optional
            Specific value of domain variable to analyze.

        Returns
        -------
        dict
            Dictionary containing model results including coefficients,
            standard errors, and p-values.

        Examples
        --------
        >>> # Linear regression
        >>> results = design.glm('TOTEXP20 ~ AGELAST + SEX')

        >>> # Logistic regression
        >>> results = design.glm('flushot ~ AGELAST + SEX + RACETHX', family='binomial')
        """
        try:
            import statsmodels.api as sm
            import statsmodels.formula.api as smf
        except ImportError:
            raise ImportError(
                "statsmodels is required for GLM analysis. "
                "Install it with: pip install statsmodels"
            )

        subset = self._get_subset(domain, domain_value)

        family_map = {
            'gaussian': sm.families.Gaussian(),
            'binomial': sm.families.Binomial(),
            'poisson': sm.families.Poisson()
        }

        if family not in family_map:
            raise ValueError(
                f"Unknown family: {family}. "
                f"Available families: {list(family_map.keys())}"
            )

        model = smf.glm(
            formula=formula,
            data=subset,
            family=family_map[family],
            freq_weights=subset[self.weights]
        )

        try:
            results = model.fit()

            return {
                'params': results.params,
                'bse': results.bse,
                'pvalues': results.pvalues,
                'conf_int': results.conf_int(),
                'nobs': results.nobs,
                'deviance': results.deviance,
                'aic': results.aic,
                'summary': results.summary()
            }
        except Exception as e:
            warnings.warn(f"Model fitting failed: {e}")
            return {'error': str(e)}


def create_pooled_design(
    data: pd.DataFrame,
    years: List[int],
    pooled_strata: str = 'STRA9619',
    pooled_cluster: str = 'PSU9619',
    weight_var: str = 'poolwt'
) -> MEPSSurveyDesign:
    """
    Create a survey design for pooled multi-year MEPS data.

    When pooling MEPS data across years (especially across the 2018 CAPI redesign),
    special variance estimation variables from the Pooled Linkage Variance file
    should be used.

    Parameters
    ----------
    data : pd.DataFrame
        Pooled MEPS data with variance linkage variables merged.
    years : list of int
        List of years included in the pooled data.
    pooled_strata : str, optional
        Pooled strata variable. Default is 'STRA9619'.
    pooled_cluster : str, optional
        Pooled PSU variable. Default is 'PSU9619'.
    weight_var : str, optional
        Pooled weight variable. Default is 'poolwt'.

    Returns
    -------
    MEPSSurveyDesign
        Survey design object for pooled analysis.

    Examples
    --------
    >>> # Pool 2017-2019 data
    >>> pooled_data = pd.concat([fyc17, fyc18, fyc19])
    >>> pooled_data['poolwt'] = pooled_data['perwt'] / 3
    >>> design = create_pooled_design(pooled_data, years=[2017, 2018, 2019])
    """
    return MEPSSurveyDesign(
        data=data,
        year=years[0],
        strata=pooled_strata,
        cluster=pooled_cluster,
        weights=weight_var,
        nest=True
    )
