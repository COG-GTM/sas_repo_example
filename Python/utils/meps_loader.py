"""
MEPS Data Loading Utilities

This module provides functions for loading MEPS (Medical Expenditure Panel Survey) 
data files in various formats including SAS V9 (.sas7bdat), SAS XPORT (.ssp), 
Stata (.dta), and ASCII (.dat) files.

For data years 2017 and later, .sas7bdat, .dta, and .xlsx formats are available.
For data years 1996-2016, .ssp (SAS XPORT) format is recommended.
"""

from pathlib import Path
from typing import Optional, Union
import pandas as pd


def load_meps_data(
    filepath: Union[str, Path],
    file_type: str = 'auto',
    encoding: str = 'latin1'
) -> pd.DataFrame:
    """
    Load MEPS data from various file formats.
    
    Parameters
    ----------
    filepath : str or Path
        Path to the MEPS data file.
    file_type : str, optional
        File type to load. Options are:
        - 'auto': Automatically detect based on file extension (default)
        - 'sas7bdat': SAS V9 format (2017+ data)
        - 'ssp': SAS XPORT format (1996-2016 data)
        - 'dta': Stata format (2017+ data)
        - 'dat': ASCII fixed-width format
        - 'xlsx': Excel format
    encoding : str, optional
        Character encoding for reading files. Default is 'latin1'.
        
    Returns
    -------
    pd.DataFrame
        DataFrame containing the MEPS data.
        
    Examples
    --------
    >>> # Load 2018 Full-Year Consolidated file (SAS V9 format)
    >>> fyc2018 = load_meps_data("C:/MEPS/h209.sas7bdat")
    
    >>> # Load 2016 file (SAS XPORT format)
    >>> fyc2016 = load_meps_data("C:/MEPS/h192.ssp")
    
    >>> # Load Stata format
    >>> fyc2018 = load_meps_data("C:/MEPS/h209.dta")
    """
    filepath = Path(filepath)
    
    if file_type == 'auto':
        suffix = filepath.suffix.lower()
        if suffix == '.sas7bdat':
            file_type = 'sas7bdat'
        elif suffix == '.ssp':
            file_type = 'ssp'
        elif suffix == '.dta':
            file_type = 'dta'
        elif suffix == '.dat':
            file_type = 'dat'
        elif suffix in ['.xlsx', '.xls']:
            file_type = 'xlsx'
        else:
            raise ValueError(f"Cannot auto-detect file type for extension: {suffix}")
    
    if file_type == 'sas7bdat':
        return _load_sas7bdat(filepath, encoding)
    elif file_type == 'ssp':
        return _load_sas_xport(filepath, encoding)
    elif file_type == 'dta':
        return _load_stata(filepath)
    elif file_type == 'dat':
        raise ValueError(
            "ASCII (.dat) files require column specifications. "
            "Use load_meps_ascii() instead with appropriate column positions."
        )
    elif file_type == 'xlsx':
        return _load_excel(filepath)
    else:
        raise ValueError(f"Unknown file type: {file_type}")


def _load_sas7bdat(filepath: Path, encoding: str = 'latin1') -> pd.DataFrame:
    """Load SAS V9 (.sas7bdat) file."""
    try:
        import pyreadstat
        df, meta = pyreadstat.read_sas7bdat(str(filepath), encoding=encoding)
        return df
    except ImportError:
        return pd.read_sas(filepath, format='sas7bdat', encoding=encoding)


def _load_sas_xport(filepath: Path, encoding: str = 'latin1') -> pd.DataFrame:
    """Load SAS XPORT (.ssp) file."""
    try:
        import pyreadstat
        df, meta = pyreadstat.read_xport(str(filepath), encoding=encoding)
        return df
    except ImportError:
        return pd.read_sas(filepath, format='xport', encoding=encoding)


def _load_stata(filepath: Path) -> pd.DataFrame:
    """Load Stata (.dta) file."""
    return pd.read_stata(filepath)


def _load_excel(filepath: Path) -> pd.DataFrame:
    """Load Excel (.xlsx) file."""
    return pd.read_excel(filepath)


def load_meps_ascii(
    filepath: Union[str, Path],
    colspecs: list,
    names: list,
    dtypes: Optional[dict] = None
) -> pd.DataFrame:
    """
    Load MEPS ASCII (.dat) fixed-width file with specified column positions.
    
    Parameters
    ----------
    filepath : str or Path
        Path to the ASCII data file.
    colspecs : list of tuples
        List of (start, end) tuples specifying column positions (0-indexed).
    names : list
        List of column names.
    dtypes : dict, optional
        Dictionary mapping column names to data types.
        
    Returns
    -------
    pd.DataFrame
        DataFrame containing the MEPS data.
        
    Examples
    --------
    >>> # Load CLNK file with specified columns
    >>> colspecs = [(0, 10), (10, 23), (23, 39), (39, 68), (68, 69), (69, 71)]
    >>> names = ['DUPERSID', 'CONDIDX', 'EVNTIDX', 'CLNKIDX', 'EVENTYPE', 'PANEL']
    >>> clnk = load_meps_ascii("C:/MEPS/h206if1.dat", colspecs, names)
    """
    filepath = Path(filepath)
    
    df = pd.read_fwf(
        filepath,
        colspecs=colspecs,
        names=names,
        dtype=dtypes
    )
    
    return df


def get_meps_file_url(filename: str, file_format: str = 'dta') -> str:
    """
    Get the download URL for a MEPS public use file.
    
    Parameters
    ----------
    filename : str
        MEPS file name (e.g., 'h209' for 2018 FYC).
    file_format : str, optional
        File format to download. Options: 'dta', 'sas7bdat', 'ssp', 'xlsx'.
        Default is 'dta'.
        
    Returns
    -------
    str
        URL for downloading the file.
        
    Examples
    --------
    >>> url = get_meps_file_url('h209', 'dta')
    >>> print(url)
    'https://meps.ahrq.gov/mepsweb/data_files/pufs/h209/h209dta.zip'
    """
    base_url = "https://meps.ahrq.gov/mepsweb/data_files/pufs"
    
    format_suffix = {
        'dta': 'dta',
        'sas7bdat': 'v9',
        'ssp': 'ssp',
        'xlsx': 'xlsx'
    }
    
    if file_format not in format_suffix:
        raise ValueError(f"Unknown format: {file_format}. Use one of: {list(format_suffix.keys())}")
    
    suffix = format_suffix[file_format]
    return f"{base_url}/{filename}/{filename}{suffix}.zip"


def download_meps_file(
    filename: str,
    output_dir: Union[str, Path],
    file_format: str = 'dta'
) -> Path:
    """
    Download and extract a MEPS public use file.
    
    Parameters
    ----------
    filename : str
        MEPS file name (e.g., 'h209' for 2018 FYC).
    output_dir : str or Path
        Directory to save the extracted file.
    file_format : str, optional
        File format to download. Default is 'dta'.
        
    Returns
    -------
    Path
        Path to the extracted file.
        
    Examples
    --------
    >>> filepath = download_meps_file('h209', 'C:/MEPS', 'dta')
    >>> fyc2018 = load_meps_data(filepath)
    """
    import urllib.request
    import zipfile
    import tempfile
    
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    url = get_meps_file_url(filename, file_format)
    
    format_ext = {
        'dta': '.dta',
        'sas7bdat': '.sas7bdat',
        'ssp': '.ssp',
        'xlsx': '.xlsx'
    }
    
    with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp:
        urllib.request.urlretrieve(url, tmp.name)
        
        with zipfile.ZipFile(tmp.name, 'r') as zip_ref:
            zip_ref.extractall(output_dir)
    
    expected_file = output_dir / f"{filename}{format_ext[file_format]}"
    if expected_file.exists():
        return expected_file
    
    for ext in format_ext.values():
        potential_file = output_dir / f"{filename}{ext}"
        if potential_file.exists():
            return potential_file
    
    raise FileNotFoundError(f"Could not find extracted file in {output_dir}")
