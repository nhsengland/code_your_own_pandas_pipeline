"""
This module provides functions to read mapping data and practice level crosstab files
from CSV files into pandas DataFrames.

Functions
---------
read_mapping_data(file_path: Optional[Path] = None) -> pd.DataFrame

read_practice_level_crosstab_files(directory: Optional[Path] = None) -> list[pd.DataFrame]
"""

from pathlib import Path
from typing import Optional

import pandas as pd
from loguru import logger
from tqdm import tqdm

from code_your_own_pandas_pipeline.config import RAW_DATA_DIR


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
