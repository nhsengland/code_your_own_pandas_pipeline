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


def read_practice_level_crosstab_files(directory: Optional[Path] = None) -> list[pd.DataFrame]:
    """
    Reads all files in the specified directory that start with 'Practice_level_crosstab'
    and returns them as a list of pandas DataFrames.
    Parameters
    ----------
    directory : Optional[Path], optional
        The directory containing the files. If not provided, the default path "RAW_DATA_DIR" will be used.
    Returns
    -------
    list[pd.DataFrame]
        A list of DataFrames, each containing the data from one of the files.
    """

    if directory is None:
        directory = RAW_DATA_DIR

    file_paths = list(directory.glob("Practice_level_crosstab*"))
    dataframes = []
    for file_path in tqdm(file_paths, desc="Reading Practice Level Crosstab Files"):
        logger.info(f"Reading file {file_path}")
        dataframes.append(pd.read_csv(file_path))

    return dataframes
