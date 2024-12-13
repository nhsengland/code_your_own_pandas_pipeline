"""
This module provides functions to read mapping data and practice level crosstab files
from CSV files into pandas DataFrames.

Functions
---------
read_mapping_data(file_path: Optional[Path] = None) -> pd.DataFrame

read_practice_level_crosstab_files(directory: Optional[Path] = None) -> list[pd.DataFrame]
"""

import os
import zipfile
from pathlib import Path
from typing import Callable, Optional, Union

import pandas as pd
import requests
from loguru import logger
from matplotlib.pylab import f
from tqdm import tqdm

from code_your_own_pandas_pipeline.config import RAW_DATA_DIR


def download_and_extract_zip(
    url: str,
    overwrite: bool = False,
    extract_path: Optional[Path] = None,
    skip_if_exists: bool = False,
) -> None:
    """
    Downloads a zip file from the given URL and extracts its contents into the RAW_DATA_DIR.

    Parameters
    ----------
    url : str
        The URL of the zip file to download.
    overwrite : bool, default False
        Whether to overwrite existing files or skip extraction if they already exist.

    Returns
    -------
    None
    """
    if not extract_path:
        extract_path = RAW_DATA_DIR

    zip_path = extract_path / "_data.zip"

    skip_logger = logger.info if skip_if_exists else logger.warning

    download_zip(url=url, zip_path=zip_path, skip_logger=skip_logger)
    extract_zip_contents(
        overwrite=overwrite, extract_path=extract_path, skip_logger=skip_logger, zip_path=zip_path
    )

    logger.success(f"Download and extraction of {url} complete!")


def extract_zip_contents(overwrite, extract_path, skip_logger, zip_path):
    """
    Extracts the contents of a zip file to a specified directory.

    Parameters
    ----------
    overwrite : bool
        If True, existing files will be overwritten. If False, existing files will be skipped.
    extract_path : pathlib.Path
        The directory where the contents of the zip file will be extracted.
    skip_logger : function
        A logging function to call when a file is skipped or overwritten.
    zip_path : pathlib.Path
        The path to the zip file to be extracted.

    Returns
    -------
    None
    """
    logger.info(f"Extracting contents of {zip_path} to {extract_path}")
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        for member in zip_ref.namelist():
            member_path = extract_path / member
            if member_path.exists():
                if not overwrite:
                    skip_logger(f"File {member_path} already exists, skipping extraction.")
                    continue
                skip_logger(f"File {member_path} already exists, overwriting.")
            zip_ref.extract(member, extract_path)

    logger.info(f"Extraction complete. Removing zip file {zip_path}")
    os.remove(zip_path)


def download_zip(
    url: str,
    overwrite: bool = False,
    zip_path: Optional[Path] = None,
    skip_logger: Callable = logger.info,
) -> None:
    """
    Downloads a zip file from a given URL to a specified path.

    Parameters
    ----------
    url : str
        The URL from which to download the zip file.
    overwrite : bool, optional
        If True, overwrite the existing file if it exists. Default is False.
    zip_path : Optional[Path], optional
        The path where the zip file should be saved. If not provided, defaults to RAW_DATA_DIR / "data.zip".
    skip_logger : Callable, optional
        A logging function to call if the file already exists and is not being overwritten. Default is logger.info.

    Returns
    -------
    None

    Raises
    ------
    requests.exceptions.RequestException
        If there is an issue with the HTTP request.
    """

    if not zip_path:
        zip_path = RAW_DATA_DIR / "data.zip"

    if zip_path.exists() and not overwrite:
        skip_logger(f"File {zip_path} already exists, skipping download.")
        return

    logger.info(f"Downloading zip file from {url} to {zip_path}")
    response = requests.get(url, stream=True, timeout=10)
    total_size = int(response.headers.get("content-length", 0))
    block_size = 1024  # 1 Kibibyte
    t = tqdm(total=total_size, unit="iB", unit_scale=True)

    with open(zip_path, "wb") as file:
        for data in response.iter_content(block_size):
            t.update(len(data))
            file.write(data)
    t.close()


def read_mapping_data(file_path: Optional[Path] = None) -> pd.DataFrame:
    """
    Reads mapping data from a CSV file and returns it as a pandas DataFrame.

    Parameters
    ----------
    file_path : Optional[Path], optional
        The path to the CSV file containing the mapping data. If not provided,
        the default path "RAW_DATA_DIR/Mapping.csv" will be used.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the mapping data from the CSV file.
    """
    if file_path is None:
        file_path = RAW_DATA_DIR / "Mapping.csv"

    logger.info(f"Reading mapping data from {file_path}")

    return pd.read_csv(file_path)


def concatenate_practice_level_data(practice_level_datasets: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Concatenates the practice data and returns the result.

    Parameters
    ----------
    practice_level_datasets : list[pd.DataFrame]
        The practice data to concatenate.
    Returns
    -------
    pd.DataFrame
        The concatenated DataFrame.
    """
    logger.info("Concatenating practice data")

    return pd.concat(practice_level_datasets, ignore_index=True)


def read_practice_level_data(
    directory: Optional[Path] = None, file_starts_with: Optional[str] = "Practice_Level_Crosstab"
) -> pd.DataFrame:
    """
    Reads and concatenates practice level data from CSV files in the specified directory.

    Parameters
    ----------
    directory : Optional[Path], default None
        The directory to search for CSV files. If None, uses RAW_DATA_DIR.
    file_starts_with : Optional[str], default "Practice_Level_Crosstab"
        The prefix of the CSV files to read.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the concatenated practice level data from the CSV files.

    Raises
    ------
    AssertionError
        If no files are found in the specified directory with the given prefix.

    Notes
    -----
    This function uses the `tqdm` library to display a progress bar while reading files.
    """
    if directory is None:
        directory = RAW_DATA_DIR

    file_paths = list(directory.glob(f"{file_starts_with}*.csv"))
    assert (
        file_paths
    ), f"No files found in {directory} with name starting with '{file_starts_with}'"

    dataframes = []
    for file_path in tqdm(file_paths, desc="Reading Practice Level Crosstab Files"):
        logger.info(f"Reading file {file_path}")
        dataframes.append(pd.read_csv(file_path))

    practice_level_data = concatenate_practice_level_data(dataframes)
    return practice_level_data
