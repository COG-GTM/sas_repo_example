"""
MEPS Python Package

Python package for analyzing Medical Expenditure Panel Survey (MEPS) data.
Migrated from the COG-GTM/sas_repo_example repository which contained R, SAS, and Stata implementations.
"""

from meps.data_loading import (
    read_meps,
    read_stata,
    read_sas_xport,
    read_sas_v9,
    download_meps_file,
    get_meps_file_name,
)

from meps.survey_design import MEPSSurveyDesign

from meps.analysis import (
    svytotal,
    svymean,
    svyby,
    svyglm,
    svyquantile,
    svyratio,
    domain_analysis,
)

from meps.utils import (
    recode_factor,
    create_indicator,
    standardize_variable_names,
    pool_weights,
)

__version__ = "0.1.0"
__all__ = [
    "read_meps",
    "read_stata",
    "read_sas_xport",
    "read_sas_v9",
    "download_meps_file",
    "get_meps_file_name",
    "MEPSSurveyDesign",
    "svytotal",
    "svymean",
    "svyby",
    "svyglm",
    "svyquantile",
    "svyratio",
    "domain_analysis",
    "recode_factor",
    "create_indicator",
    "standardize_variable_names",
    "pool_weights",
]
