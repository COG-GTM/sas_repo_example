"""
MEPS Data Loading Module

Python equivalent of R's MEPS package read_MEPS() function and related data loading utilities.
Supports loading MEPS data files from various formats and years.
"""

import os
import tempfile
from io import BytesIO
from typing import Optional, Union
from zipfile import ZipFile

import pandas as pd
import pyreadstat
import requests

MEPS_BASE_URL = "https://meps.ahrq.gov/mepsweb/data_files/pufs"

MEPS_FILE_NAMES = {
    1996: {"FYC": "h12", "Conditions": "h06r", "PMED": "h10a", "Events": "h10", "Jobs": "h07", "CLNK": "h10if1"},
    1997: {"FYC": "h20", "Conditions": "h18", "PMED": "h16a", "Events": "h16", "Jobs": "h19", "CLNK": "h16if1"},
    1998: {"FYC": "h28", "Conditions": "h27", "PMED": "h26a", "Events": "h26", "Jobs": "h25", "CLNK": "h26if1"},
    1999: {"FYC": "h38", "Conditions": "h37", "PMED": "h33a", "Events": "h33", "Jobs": "h32", "CLNK": "h33if1"},
    2000: {"FYC": "h50", "Conditions": "h52", "PMED": "h51a", "Events": "h51", "Jobs": "h40", "CLNK": "h51if1"},
    2001: {"FYC": "h60", "Conditions": "h61", "PMED": "h59a", "Events": "h59", "Jobs": "h56", "CLNK": "h59if1"},
    2002: {"FYC": "h70", "Conditions": "h69", "PMED": "h67a", "Events": "h67", "Jobs": "h63", "CLNK": "h67if1"},
    2003: {"FYC": "h79", "Conditions": "h78", "PMED": "h77a", "Events": "h77", "Jobs": "h74", "CLNK": "h77if1"},
    2004: {"FYC": "h89", "Conditions": "h87", "PMED": "h85a", "Events": "h85", "Jobs": "h83", "CLNK": "h85if1"},
    2005: {"FYC": "h97", "Conditions": "h96", "PMED": "h94a", "Events": "h94", "Jobs": "h91", "CLNK": "h94if1"},
    2006: {"FYC": "h105", "Conditions": "h104", "PMED": "h102a", "Events": "h102", "Jobs": "h100", "CLNK": "h102if1"},
    2007: {"FYC": "h113", "Conditions": "h112", "PMED": "h110a", "Events": "h110", "Jobs": "h108", "CLNK": "h110if1"},
    2008: {"FYC": "h121", "Conditions": "h120", "PMED": "h118a", "Events": "h118", "Jobs": "h116", "CLNK": "h118if1"},
    2009: {"FYC": "h129", "Conditions": "h128", "PMED": "h126a", "Events": "h126", "Jobs": "h124", "CLNK": "h126if1"},
    2010: {"FYC": "h138", "Conditions": "h137", "PMED": "h135a", "Events": "h135", "Jobs": "h133", "CLNK": "h135if1"},
    2011: {"FYC": "h147", "Conditions": "h146", "PMED": "h144a", "Events": "h144", "Jobs": "h142", "CLNK": "h144if1"},
    2012: {"FYC": "h155", "Conditions": "h154", "PMED": "h152a", "Events": "h152", "Jobs": "h150", "CLNK": "h152if1"},
    2013: {"FYC": "h163", "Conditions": "h162", "PMED": "h160a", "Events": "h160", "Jobs": "h158", "CLNK": "h160if1"},
    2014: {"FYC": "h171", "Conditions": "h170", "PMED": "h168a", "Events": "h168", "Jobs": "h166", "CLNK": "h168if1"},
    2015: {"FYC": "h181", "Conditions": "h180", "PMED": "h178a", "Events": "h178", "Jobs": "h176", "CLNK": "h178if1"},
    2016: {"FYC": "h192", "Conditions": "h190", "PMED": "h188a", "Events": "h188", "Jobs": "h185", "CLNK": "h188if1"},
    2017: {"FYC": "h201", "Conditions": "h199", "PMED": "h197a", "Events": "h197", "Jobs": "h195", "CLNK": "h197if1"},
    2018: {"FYC": "h209", "Conditions": "h207", "PMED": "h206a", "Events": "h206", "Jobs": "h203", "CLNK": "h206if1"},
    2019: {"FYC": "h216", "Conditions": "h214", "PMED": "h213a", "Events": "h213", "Jobs": "h211", "CLNK": "h213if1"},
    2020: {"FYC": "h224", "Conditions": "h222", "PMED": "h220a", "Events": "h220", "Jobs": "h218", "CLNK": "h220if1"},
    2021: {"FYC": "h233", "Conditions": "h231", "PMED": "h229a", "Events": "h229", "Jobs": "h227", "CLNK": "h229if1"},
    2022: {"FYC": "h243", "Conditions": "h241", "PMED": "h239a", "Events": "h239", "Jobs": "h237", "CLNK": "h239i"},
}

EVENT_TYPE_SUFFIXES = {
    "DV": "b",
    "OT": "c",
    "IP": "d",
    "ER": "e",
    "OP": "f",
    "OB": "g",
    "HH": "h",
}

POOLED_LINKAGE_FILE = "h36u19"


def get_meps_file_name(
    year: Optional[int] = None,
    file_type: Optional[str] = None,
    event_type: Optional[str] = None,
) -> str:
    """
    Get the MEPS file name for a given year and type.

    Args:
        year: Data year (e.g., 2018)
        file_type: File type ('FYC', 'Conditions', 'PMED', 'Events', 'Jobs', 'CLNK')
        event_type: Event type for event files ('DV', 'OT', 'IP', 'ER', 'OP', 'OB', 'HH')

    Returns:
        MEPS file name (e.g., 'h209')
    """
    if file_type == "Pooled linkage":
        return POOLED_LINKAGE_FILE

    if year not in MEPS_FILE_NAMES:
        raise ValueError(f"Year {year} not found in MEPS file names. Valid years: {list(MEPS_FILE_NAMES.keys())}")

    year_files = MEPS_FILE_NAMES[year]

    if file_type in year_files:
        base_name = year_files[file_type]
        if file_type == "Events" and event_type:
            suffix = EVENT_TYPE_SUFFIXES.get(event_type, "")
            return f"{base_name}{suffix}"
        return base_name

    type_mapping = {
        "COND": "Conditions",
        "PM": "PMED",
        "RX": "PMED",
    }
    mapped_type = type_mapping.get(file_type, file_type)
    if mapped_type in year_files:
        return year_files[mapped_type]

    raise ValueError(f"File type '{file_type}' not found for year {year}. Valid types: {list(year_files.keys())}")


def read_stata(filepath: str) -> pd.DataFrame:
    """
    Read Stata .dta files.

    Args:
        filepath: Path to the .dta file

    Returns:
        pandas DataFrame with the data
    """
    df, meta = pyreadstat.read_dta(filepath)
    return df


def read_sas_xport(filepath: str) -> pd.DataFrame:
    """
    Read SAS XPORT .ssp files for 1996-2016 data.

    Args:
        filepath: Path to the .ssp file

    Returns:
        pandas DataFrame with the data
    """
    df, meta = pyreadstat.read_xport(filepath)
    return df


def read_sas_v9(filepath: str) -> pd.DataFrame:
    """
    Read SAS V9 .sas7bdat files for 2017+ data.

    Args:
        filepath: Path to the .sas7bdat file

    Returns:
        pandas DataFrame with the data
    """
    df, meta = pyreadstat.read_sas7bdat(filepath)
    return df


def read_excel(filepath: str) -> pd.DataFrame:
    """
    Read Excel .xlsx files.

    Args:
        filepath: Path to the .xlsx file

    Returns:
        pandas DataFrame with the data
    """
    return pd.read_excel(filepath)


def _get_format_url(file_name: str, file_format: str) -> str:
    """
    Get the URL for downloading a MEPS file in a specific format.

    Args:
        file_name: MEPS file name (e.g., 'h209')
        file_format: File format ('dta', 'sas7bdat', 'ssp', 'xlsx')

    Returns:
        URL for downloading the file
    """
    format_suffixes = {
        "dta": "dta",
        "sas7bdat": "v9",
        "ssp": "ssp",
        "xlsx": "xlsx",
    }
    suffix = format_suffixes.get(file_format, file_format)
    return f"{MEPS_BASE_URL}/{file_name}/{file_name}{suffix}.zip"


def download_meps_file(
    file_name: str,
    file_format: str = "dta",
    save_dir: Optional[str] = None,
) -> str:
    """
    Download MEPS files directly from the MEPS website.

    Args:
        file_name: MEPS file name (e.g., 'h209')
        file_format: File format ('dta', 'sas7bdat', 'ssp', 'xlsx')
        save_dir: Directory to save files (uses temp directory if None)

    Returns:
        Path to the downloaded file
    """
    url = _get_format_url(file_name, file_format)

    response = requests.get(url, timeout=120)
    response.raise_for_status()

    if save_dir is None:
        save_dir = tempfile.mkdtemp()

    with ZipFile(BytesIO(response.content)) as zip_file:
        zip_file.extractall(save_dir)

    extension_map = {
        "dta": ".dta",
        "sas7bdat": ".sas7bdat",
        "ssp": ".ssp",
        "xlsx": ".xlsx",
    }
    extension = extension_map.get(file_format, f".{file_format}")

    for root, dirs, files in os.walk(save_dir):
        for f in files:
            if f.lower().endswith(extension):
                return os.path.join(root, f)

    raise FileNotFoundError(f"Could not find {extension} file in downloaded archive")


def read_meps(
    file: Optional[str] = None,
    year: Optional[int] = None,
    type: Optional[str] = None,
    event_type: Optional[str] = None,
    local_dir: Optional[str] = None,
) -> pd.DataFrame:
    """
    Load MEPS data files - Python equivalent of R's MEPS::read_MEPS().

    This function automatically detects the best file format to import based on
    the specified data year and file. For data years 2017 and later, Stata (.dta)
    files are used. For data years 1996-2016, SAS XPORT (.ssp) files are used.

    Args:
        file: MEPS file name (e.g., 'h206b'). If provided, year and type are ignored.
        year: Data year (e.g., 2018)
        type: File type ('FYC', 'DV', 'PM', 'COND', 'OB', 'OP', 'ER', 'IP', 'HH', 'CLNK', etc.)
        event_type: Event type for event files (alternative to specifying in type)
        local_dir: Local directory path containing MEPS files

    Returns:
        pandas DataFrame with the MEPS data

    Examples:
        >>> fyc = read_meps(year=2018, type='FYC')
        >>> dn2018 = read_meps(file='h206b')
        >>> ob2020 = read_meps(year=2020, type='OB')
    """
    if file is None:
        if year is None or type is None:
            raise ValueError("Either 'file' or both 'year' and 'type' must be provided")

        if type == "Pooled linkage":
            file = POOLED_LINKAGE_FILE
            year = 2019
        elif type in EVENT_TYPE_SUFFIXES:
            file = get_meps_file_name(year, "Events", type)
        else:
            file = get_meps_file_name(year, type, event_type)

    if year is None:
        year = _infer_year_from_file(file)

    if local_dir:
        return _load_from_local(file, year, local_dir)

    if year >= 2017 or (year == 2016 and "h190" in file):
        file_format = "dta"
        reader = read_stata
    else:
        file_format = "ssp"
        reader = read_sas_xport

    filepath = download_meps_file(file, file_format)
    return reader(filepath)


def _infer_year_from_file(file_name: str) -> int:
    """
    Infer the year from a MEPS file name.

    Args:
        file_name: MEPS file name (e.g., 'h209')

    Returns:
        Inferred year
    """
    base_name = file_name.rstrip("abcdefgh").rstrip("if1").rstrip("if2")

    for year, files in MEPS_FILE_NAMES.items():
        for file_type, name in files.items():
            if name == base_name or file_name.startswith(name):
                return year

    return 2020


def _load_from_local(file_name: str, year: int, local_dir: str) -> pd.DataFrame:
    """
    Load a MEPS file from a local directory.

    Args:
        file_name: MEPS file name
        year: Data year
        local_dir: Local directory path

    Returns:
        pandas DataFrame with the data
    """
    extensions = [".dta", ".sas7bdat", ".ssp", ".xlsx"]

    for ext in extensions:
        filepath = os.path.join(local_dir, f"{file_name}{ext}")
        if os.path.exists(filepath):
            if ext == ".dta":
                return read_stata(filepath)
            elif ext == ".sas7bdat":
                return read_sas_v9(filepath)
            elif ext == ".ssp":
                return read_sas_xport(filepath)
            elif ext == ".xlsx":
                return read_excel(filepath)

    raise FileNotFoundError(f"Could not find {file_name} in {local_dir}")
