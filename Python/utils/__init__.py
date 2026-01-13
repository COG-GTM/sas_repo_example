"""
MEPS Python Utilities

This package provides utilities for loading and analyzing MEPS 
(Medical Expenditure Panel Survey) data in Python.

Modules
-------
meps_loader
    Functions for loading MEPS data files in various formats.
survey_design
    Survey design class for weighted analysis.
"""

from .meps_loader import (
    load_meps_data,
    load_meps_ascii,
    get_meps_file_url,
    download_meps_file
)

from .survey_design import (
    MEPSSurveyDesign,
    create_pooled_design
)

__all__ = [
    'load_meps_data',
    'load_meps_ascii',
    'get_meps_file_url',
    'download_meps_file',
    'MEPSSurveyDesign',
    'create_pooled_design'
]
