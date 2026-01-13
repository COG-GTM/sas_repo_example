"""
MEPS Data Loader

This module provides functions for loading MEPS (Medical Expenditure Panel Survey)
data files in various formats including SAS V9 (.sas7bdat), SAS XPORT (.ssp),
Stata (.dta), and ASCII (.dat) formats.

For data years 2017 and later, .dta (Stata) or .sas7bdat (SAS V9) formats are recommended.
For data years 1996-2016, .ssp (SAS XPORT) format is recommended.
"""

import os
import tempfile
import urllib.request
import zipfile
from typing import Optional, Dict, Any

import pandas as pd


def load_meps_data(
    filepath: str,
    file_type: str = 'auto',
    column_specs: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """
    Load MEPS data from various file formats.

    Parameters
    ----------
    filepath : str
        Path to the MEPS data file.
    file_type : str, optional
        File type to load. Options are 'auto', 'dta', 'sas7bdat', 'ssp', 'dat'.
        Default is 'auto' which detects the file type from the extension.
    column_specs : dict, optional
        For ASCII (.dat) files, a dictionary containing:
        - 'colspecs': list of tuples with (start, end) positions
        - 'names': list of column names
        - 'dtypes': dict of column data types (optional)

    Returns
    -------
    pd.DataFrame
        The loaded MEPS data as a pandas DataFrame.

    Examples
    --------
    >>> # Load Stata file (recommended for 2017+)
    >>> data = load_meps_data("C:/MEPS/h224.dta")

    >>> # Load SAS V9 file
    >>> data = load_meps_data("C:/MEPS/h224.sas7bdat")

    >>> # Load SAS XPORT file (for 1996-2016 data)
    >>> data = load_meps_data("C:/MEPS/h188a.ssp")

    >>> # Load ASCII file with column specifications
    >>> specs = {
    ...     'colspecs': [(0, 10), (10, 23), (23, 39)],
    ...     'names': ['DUPERSID', 'CONDIDX', 'EVNTIDX']
    ... }
    >>> data = load_meps_data("C:/MEPS/h206if1.dat", column_specs=specs)
    """
    if file_type == 'auto':
        file_type = _detect_file_type(filepath)

    if file_type == 'dta':
        return _load_stata(filepath)
    elif file_type == 'sas7bdat':
        return _load_sas7bdat(filepath)
    elif file_type == 'ssp':
        return _load_sas_xport(filepath)
    elif file_type == 'dat':
        if column_specs is None:
            raise ValueError(
                "column_specs must be provided for ASCII (.dat) files. "
                "Include 'colspecs' (list of tuples) and 'names' (list of column names)."
            )
        return _load_ascii(filepath, column_specs)
    else:
        raise ValueError(
            f"Unsupported file type: {file_type}. "
            "Supported types are: 'dta', 'sas7bdat', 'ssp', 'dat'"
        )


def _detect_file_type(filepath: str) -> str:
    """Detect file type from file extension."""
    ext = os.path.splitext(filepath)[1].lower()
    type_map = {
        '.dta': 'dta',
        '.sas7bdat': 'sas7bdat',
        '.ssp': 'ssp',
        '.dat': 'dat'
    }
    if ext not in type_map:
        raise ValueError(
            f"Cannot detect file type from extension '{ext}'. "
            "Please specify file_type explicitly."
        )
    return type_map[ext]


def _load_stata(filepath: str) -> pd.DataFrame:
    """Load Stata (.dta) file."""
    return pd.read_stata(filepath)


def _load_sas7bdat(filepath: str) -> pd.DataFrame:
    """Load SAS V9 (.sas7bdat) file."""
    try:
        import pyreadstat
        df, _ = pyreadstat.read_sas7bdat(filepath)
        return df
    except ImportError:
        return pd.read_sas(filepath, format='sas7bdat')


def _load_sas_xport(filepath: str) -> pd.DataFrame:
    """Load SAS XPORT (.ssp) file."""
    try:
        import pyreadstat
        df, _ = pyreadstat.read_xport(filepath)
        return df
    except ImportError:
        return pd.read_sas(filepath, format='xport')


def _load_ascii(filepath: str, column_specs: Dict[str, Any]) -> pd.DataFrame:
    """Load ASCII (.dat) fixed-width file."""
    colspecs = column_specs.get('colspecs')
    names = column_specs.get('names')
    dtypes = column_specs.get('dtypes', None)

    if colspecs is None or names is None:
        raise ValueError(
            "column_specs must include 'colspecs' and 'names' keys."
        )

    df = pd.read_fwf(filepath, colspecs=colspecs, names=names)

    if dtypes:
        for col, dtype in dtypes.items():
            if col in df.columns:
                df[col] = df[col].astype(dtype)

    return df


def download_meps_file(
    file_name: str,
    file_format: str = 'dta',
    save_dir: Optional[str] = None
) -> str:
    """
    Download a MEPS data file from the MEPS website.

    Parameters
    ----------
    file_name : str
        The MEPS file name (e.g., 'h224' for 2020 FYC file).
    file_format : str, optional
        The file format to download. Options are 'dta', 'sas7bdat', 'ssp'.
        Default is 'dta'.
    save_dir : str, optional
        Directory to save the downloaded file. If None, uses a temporary directory.

    Returns
    -------
    str
        Path to the downloaded file.

    Examples
    --------
    >>> # Download 2020 FYC file in Stata format
    >>> filepath = download_meps_file('h224', 'dta')
    >>> data = load_meps_data(filepath)
    """
    format_map = {
        'dta': ('dta', 'dta'),
        'sas7bdat': ('v9', 'sas7bdat'),
        'ssp': ('ssp', 'ssp')
    }

    if file_format not in format_map:
        raise ValueError(
            f"Unsupported format: {file_format}. "
            "Supported formats are: 'dta', 'sas7bdat', 'ssp'"
        )

    url_suffix, file_ext = format_map[file_format]

    base_url = "https://meps.ahrq.gov/mepsweb/data_files/pufs"
    zip_url = f"{base_url}/{file_name}/{file_name}{url_suffix}.zip"

    if save_dir is None:
        save_dir = tempfile.mkdtemp()

    zip_path = os.path.join(save_dir, f"{file_name}.zip")

    urllib.request.urlretrieve(zip_url, zip_path)

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(save_dir)

    file_path = os.path.join(save_dir, f"{file_name}.{file_ext}")

    if not os.path.exists(file_path):
        for f in os.listdir(save_dir):
            if f.endswith(f".{file_ext}"):
                file_path = os.path.join(save_dir, f)
                break

    return file_path


def get_meps_file_info(year: int, data_type: str) -> Dict[str, str]:
    """
    Get MEPS file information for a given year and data type.

    Parameters
    ----------
    year : int
        The data year (e.g., 2020).
    data_type : str
        The type of data file. Common options include:
        - 'FYC': Full-Year Consolidated
        - 'Conditions': Medical Conditions
        - 'PMED': Prescribed Medicines Events
        - 'OB': Office-Based Medical Provider Visits
        - 'OP': Outpatient Visits
        - 'ER': Emergency Room Visits
        - 'IP': Hospital Inpatient Stays
        - 'DV': Dental Visits
        - 'HH': Home Health
        - 'CLNK': Condition-Event Link

    Returns
    -------
    dict
        Dictionary with file information including 'file_name' and 'recommended_format'.

    Examples
    --------
    >>> info = get_meps_file_info(2020, 'FYC')
    >>> print(info['file_name'])  # 'h224'
    """
    fyc_files = {
        2020: 'h224', 2019: 'h216', 2018: 'h209', 2017: 'h201',
        2016: 'h192', 2015: 'h181', 2014: 'h171', 2013: 'h163'
    }

    conditions_files = {
        2020: 'h222', 2019: 'h214', 2018: 'h207', 2017: 'h199',
        2016: 'h190', 2015: 'h180', 2014: 'h170', 2013: 'h162'
    }

    pmed_files = {
        2020: 'h220a', 2019: 'h213a', 2018: 'h206a', 2017: 'h197a',
        2016: 'h188a', 2015: 'h178a', 2014: 'h168a', 2013: 'h160a'
    }

    ob_files = {
        2020: 'h220g', 2019: 'h213g', 2018: 'h206g', 2017: 'h197g',
        2016: 'h188g', 2015: 'h178g', 2014: 'h168g', 2013: 'h160g'
    }

    clnk_files = {
        2020: 'h220if1', 2019: 'h213if1', 2018: 'h206if1', 2017: 'h197if1'
    }

    file_maps = {
        'FYC': fyc_files,
        'Conditions': conditions_files,
        'PMED': pmed_files,
        'OB': ob_files,
        'CLNK': clnk_files
    }

    if data_type not in file_maps:
        raise ValueError(
            f"Unknown data type: {data_type}. "
            f"Available types: {list(file_maps.keys())}"
        )

    if year not in file_maps[data_type]:
        raise ValueError(
            f"No file available for year {year} and type {data_type}."
        )

    file_name = file_maps[data_type][year]
    recommended_format = 'dta' if year >= 2017 else 'ssp'

    return {
        'file_name': file_name,
        'year': year,
        'data_type': data_type,
        'recommended_format': recommended_format
    }
