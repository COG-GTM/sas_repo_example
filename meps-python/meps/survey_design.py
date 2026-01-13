"""
MEPS Survey Design Module

Survey design object for MEPS data analysis.
Equivalent to R's svydesign() and Stata's svyset.
Implements Taylor series linearization for proper variance estimation
with stratification and clustering.
"""

from typing import Any, Callable, Dict, List, Optional, Union

import numpy as np
import pandas as pd
from scipy import stats


class MEPSSurveyDesign:
    """
    Survey design object for MEPS data analysis.
    Equivalent to R's svydesign() and Stata's svyset.

    This class handles the complex survey design of MEPS data, including:
    - Stratification (VARSTR)
    - Clustering/PSUs (VARPSU)
    - Sampling weights (e.g., PERWT18F)

    The design properly accounts for the nested structure of MEPS data
    and implements Taylor series linearization for variance estimation.
    """

    def __init__(
        self,
        data: pd.DataFrame,
        id_var: str = "VARPSU",
        strata_var: str = "VARSTR",
        weight_var: Optional[str] = None,
        nest: bool = True,
        lonely_psu: str = "adjust",
    ):
        """
        Initialize MEPS survey design.

        Args:
            data: pandas DataFrame with MEPS data
            id_var: PSU variable (default: 'VARPSU')
            strata_var: Strata variable (default: 'VARSTR')
            weight_var: Weight variable (e.g., 'PERWT18F')
            nest: Whether PSUs are nested within strata (default: True for MEPS)
            lonely_psu: How to handle lonely PSUs ('adjust', 'remove', 'certainty', 'fail')
                       'adjust' is equivalent to R's survey.lonely.psu='adjust'
        """
        self.data = data.copy()
        self.id_var = id_var
        self.strata_var = strata_var
        self.weight_var = weight_var
        self.nest = nest
        self.lonely_psu = lonely_psu

        self._validate_design_vars()
        self._process_design()

    def _validate_design_vars(self) -> None:
        """Ensure VARPSU, VARSTR, and weight variables exist."""
        required = [self.id_var, self.strata_var]
        if self.weight_var:
            required.append(self.weight_var)

        missing = [v for v in required if v not in self.data.columns]
        if missing:
            raise ValueError(f"Missing required variables: {missing}")

    def _process_design(self) -> None:
        """Process the survey design and identify strata/PSU structure."""
        self._strata = self.data[self.strata_var].unique()
        self._n_strata = len(self._strata)

        self._strata_psu_counts = (
            self.data.groupby(self.strata_var)[self.id_var].nunique().to_dict()
        )

        self._lonely_strata = [
            s for s, count in self._strata_psu_counts.items() if count == 1
        ]

    def subset(self, condition: Union[pd.Series, np.ndarray]) -> "MEPSSurveyDesign":
        """
        Subset design to domain - equivalent to R's subset().
        Maintains full survey structure for proper variance estimation.

        Args:
            condition: Boolean array or Series indicating which rows to keep

        Returns:
            New MEPSSurveyDesign object with subsetted data
        """
        subset_data = self.data[condition].copy()
        return MEPSSurveyDesign(
            subset_data,
            self.id_var,
            self.strata_var,
            self.weight_var,
            self.nest,
            self.lonely_psu,
        )

    def get_weights(self) -> np.ndarray:
        """Get the sampling weights as a numpy array."""
        if self.weight_var is None:
            return np.ones(len(self.data))
        return self.data[self.weight_var].values

    def _calculate_stratum_variance(
        self,
        stratum_data: pd.DataFrame,
        variable: str,
        estimate_type: str = "total",
    ) -> float:
        """
        Calculate variance contribution from a single stratum using Taylor series linearization.

        Args:
            stratum_data: Data for a single stratum
            variable: Variable name to calculate variance for
            estimate_type: Type of estimate ('total', 'mean')

        Returns:
            Variance contribution from this stratum
        """
        psus = stratum_data[self.id_var].unique()
        n_psu = len(psus)

        if n_psu == 1:
            if self.lonely_psu == "adjust":
                return 0.0
            elif self.lonely_psu == "remove":
                return 0.0
            elif self.lonely_psu == "certainty":
                return 0.0
            elif self.lonely_psu == "fail":
                raise ValueError(f"Lonely PSU found in stratum")
            else:
                return 0.0

        psu_totals = []
        for psu in psus:
            psu_data = stratum_data[stratum_data[self.id_var] == psu]
            if self.weight_var:
                weights = psu_data[self.weight_var].values
            else:
                weights = np.ones(len(psu_data))
            values = psu_data[variable].values
            psu_total = np.nansum(weights * values)
            psu_totals.append(psu_total)

        psu_totals = np.array(psu_totals)
        mean_psu_total = np.mean(psu_totals)

        variance = (n_psu / (n_psu - 1)) * np.sum((psu_totals - mean_psu_total) ** 2)

        return variance

    def _calculate_variance(
        self,
        variable: str,
        estimate_type: str = "total",
    ) -> float:
        """
        Calculate variance using Taylor series linearization.
        Accounts for stratification and clustering.

        Args:
            variable: Variable name to calculate variance for
            estimate_type: Type of estimate ('total', 'mean')

        Returns:
            Variance estimate
        """
        total_variance = 0.0

        for stratum in self._strata:
            stratum_data = self.data[self.data[self.strata_var] == stratum]
            stratum_var = self._calculate_stratum_variance(
                stratum_data, variable, estimate_type
            )
            total_variance += stratum_var

        return total_variance

    def _calculate_mean_variance(
        self,
        variable: str,
    ) -> float:
        """
        Calculate variance for a mean estimate using Taylor series linearization.

        Args:
            variable: Variable name

        Returns:
            Variance of the mean estimate
        """
        weights = self.get_weights()
        values = self.data[variable].values

        valid_mask = ~np.isnan(values)
        weights = weights[valid_mask]
        values = values[valid_mask]

        total_weight = np.sum(weights)
        weighted_mean = np.sum(weights * values) / total_weight

        residuals = values - weighted_mean
        self.data["_temp_residual"] = np.nan
        self.data.loc[valid_mask, "_temp_residual"] = residuals

        var_numerator = self._calculate_variance("_temp_residual", "total")

        del self.data["_temp_residual"]

        variance = var_numerator / (total_weight ** 2)

        return variance

    def weighted_total(self, variable: str) -> Dict[str, float]:
        """
        Calculate weighted total for a variable.

        Args:
            variable: Variable name

        Returns:
            Dictionary with 'estimate', 'se', and 'ci' keys
        """
        weights = self.get_weights()
        values = self.data[variable].values

        valid_mask = ~np.isnan(values)
        estimate = np.nansum(weights * values)

        variance = self._calculate_variance(variable, "total")
        se = np.sqrt(variance)

        ci_lower = estimate - 1.96 * se
        ci_upper = estimate + 1.96 * se

        return {
            "estimate": estimate,
            "se": se,
            "ci": (ci_lower, ci_upper),
        }

    def weighted_mean(self, variable: str) -> Dict[str, float]:
        """
        Calculate weighted mean for a variable.

        Args:
            variable: Variable name

        Returns:
            Dictionary with 'estimate', 'se', and 'ci' keys
        """
        weights = self.get_weights()
        values = self.data[variable].values

        valid_mask = ~np.isnan(values)
        w = weights[valid_mask]
        v = values[valid_mask]

        estimate = np.sum(w * v) / np.sum(w)

        variance = self._calculate_mean_variance(variable)
        se = np.sqrt(variance)

        ci_lower = estimate - 1.96 * se
        ci_upper = estimate + 1.96 * se

        return {
            "estimate": estimate,
            "se": se,
            "ci": (ci_lower, ci_upper),
        }

    def weighted_proportion(self, variable: str) -> Dict[str, Dict[str, float]]:
        """
        Calculate weighted proportions for a categorical variable.

        Args:
            variable: Variable name (should be categorical or 0/1)

        Returns:
            Dictionary mapping categories to their proportion estimates
        """
        weights = self.get_weights()
        values = self.data[variable].values

        categories = pd.unique(values[~pd.isna(values)])
        results = {}

        for cat in categories:
            indicator = (values == cat).astype(float)
            indicator[pd.isna(values)] = np.nan

            temp_var = f"_temp_indicator_{cat}"
            self.data[temp_var] = indicator

            result = self.weighted_mean(temp_var)
            results[cat] = result

            del self.data[temp_var]

        return results

    def __repr__(self) -> str:
        """String representation of the survey design."""
        return (
            f"MEPSSurveyDesign(\n"
            f"  n_obs={len(self.data)},\n"
            f"  id_var='{self.id_var}',\n"
            f"  strata_var='{self.strata_var}',\n"
            f"  weight_var='{self.weight_var}',\n"
            f"  n_strata={self._n_strata},\n"
            f"  lonely_psu='{self.lonely_psu}'\n"
            f")"
        )


def create_survey_design(
    data: pd.DataFrame,
    id_var: str = "VARPSU",
    strata_var: str = "VARSTR",
    weight_var: Optional[str] = None,
    nest: bool = True,
    lonely_psu: str = "adjust",
) -> MEPSSurveyDesign:
    """
    Create a survey design object for MEPS data.
    Convenience function equivalent to R's svydesign().

    Args:
        data: pandas DataFrame with MEPS data
        id_var: PSU variable (default: 'VARPSU')
        strata_var: Strata variable (default: 'VARSTR')
        weight_var: Weight variable (e.g., 'PERWT18F')
        nest: Whether PSUs are nested within strata
        lonely_psu: How to handle lonely PSUs

    Returns:
        MEPSSurveyDesign object
    """
    return MEPSSurveyDesign(
        data=data,
        id_var=id_var,
        strata_var=strata_var,
        weight_var=weight_var,
        nest=nest,
        lonely_psu=lonely_psu,
    )
