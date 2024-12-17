"""
This module contains the function to read the mapping and practice crosstab data from the data
folder.
"""

import pandas as pd
from loguru import logger


def read_mapping_data() -> pd.DataFrame:
    """
    Read the mapping data from the data folder.

    Returns
    -------
    pd.DataFrame
        The mapping data.
    """
    logger.info(f"Reading mapping data from {""}")

    logger.warning("This function is not yet implemented.")


def read_practice_crosstab_data() -> pd.DataFrame:
    """
    Read the practice crosstab data from the data folder.

    Returns
    -------
    pd.DataFrame
        The practice crosstab data.
    """
    logger.info(f"Reading practice crosstab data from {""}")

    logger.warning("This function is not yet implemented.")


if __name__ == "__main__":
    read_mapping_data().head()
    read_practice_crosstab_data().head()
