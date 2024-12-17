"""
This module contains the functions to process the mapping and practice crosstab data and merge them.
"""

import pandas as pd
from loguru import logger

placeholder_df = pd.DataFrame()


def tidy_practice_level_data(practice_data: pd.DataFrame) -> pd.DataFrame:
    """
    Tidy the practice crosstab data.

    Parameters
    ----------
    practice_crosstab : pd.DataFrame
        The practice crosstab data.

    Returns
    -------
    pd.DataFrame
        The tidy practice crosstab data.
    """
    logger.info("Tidying the practice crosstab data.")

    logger.warning("This function is not yet implemented.")


def merge_mapping_and_practice_data(
    mapping_data: pd.DataFrame, practice_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Merge the mapping and practice data.

    Parameters
    ----------
    mapping_data : pd.DataFrame
        The mapping data.
    practice_data : pd.DataFrame
        The practice data.

    Returns
    -------
    pd.DataFrame
        The merged data.
    """
    logger.info("Merging the mapping and practice data.")

    logger.warning("This function is not yet implemented.")
