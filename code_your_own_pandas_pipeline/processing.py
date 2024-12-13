"""
This module provides functions to process and merge practice level data with mapping data.

Functions:
    tidy_practice_level_data(data: pd.DataFrame) -> pd.DataFrame:
        Cleans and tidies the practice level data by performing necessary transformations and validations.

    check_merge(data1: pd.DataFrame, data2: pd.DataFrame) -> bool:
        Checks if the merge between two datasets can be performed without any issues such as duplicate keys or missing values.

    merge_mapping_data_with_practice_level_data(mapping_data: pd.DataFrame, practice_data: pd.DataFrame) -> pd.DataFrame:
        Merges the mapping data with the practice level data, ensuring that the resulting dataset is consistent and complete.
"""

import pandas as pd
from loguru import logger

from code_your_own_pandas_pipeline.config import INTERIM_DATA_DIR


def tidy_practice_level_data(
    practice_level_data: pd.DataFrame, _save_interim_output: bool = False
) -> pd.DataFrame:
    """
    Processes the practice data and returns the result.

    Parameters
    ----------
    practice_level_data : pd.DataFrame
        The practice data to process.
    _save_interim_output : bool, optional
        Whether to save the interim output, by default False

    Returns
    -------
    pd.DataFrame
        The processed DataFrame.
    """
    logger.info("Processing practice data")

    logger.info("Selecting columns: 'APPOINTMENT_MONTH_START_DATE', 'GP_CODE' and 6 other columns")
    practice_level_data = practice_level_data.loc[
        :,
        [
            "APPOINTMENT_MONTH_START_DATE",
            "GP_CODE",
            "HCP_TYPE",
            "APPT_MODE",
            "NATIONAL_CATEGORY",
            "TIME_BETWEEN_BOOK_AND_APPT",
            "COUNT_OF_APPOINTMENTS",
            "APPT_STATUS",
        ],
    ]

    # We are commenting out this code as we want to test out the pipeline without loosing this data
    # logger.info('Change "Unknown" to missing data')
    # practice_level_data.replace("Unknown", None, inplace=True)

    # logger.info("Dropping rows where APPT_STATUS is missing")
    # practice_level_data.dropna(subset=["APPT_STATUS"], inplace=True)

    logger.info("Convert APPOINTMENT_MONTH_START_DATE to datetime")
    practice_level_data["APPOINTMENT_MONTH_START_DATE"] = pd.to_datetime(
        practice_level_data["APPOINTMENT_MONTH_START_DATE"], format="%d%b%Y"
    )

    if _save_interim_output:
        save_path = INTERIM_DATA_DIR / "processed_practice_level_data.csv"
        logger.info(f"Saving interim output to {save_path}")
        practice_level_data.to_csv(save_path, index=False)

    return practice_level_data


def check_merge(merged_df: pd.DataFrame, merge_column: str = "_merge") -> pd.DataFrame:
    """
    Check the merge for any issues and return the merged DataFrame.

    Parameters
    ----------
    merged_df : pd.DataFrame
        The merged DataFrame to check.
    merge_column : str, optional
        The column to check for issues, by default "_merge"

    Returns
    -------
    pd.DataFrame
        The merged DataFrame.
    """
    bad_merge = False
    for bad_merge in ("left_only", "right_only"):
        bad_merge_count = merged_df[merge_column].value_counts().get(bad_merge, 0)
        if bad_merge_count:
            logger.warning(f"There are {bad_merge_count} '{bad_merge}' rows in the merged data")
            bad_merge = True

    if not bad_merge:
        logger.info("The merge was healthy.")

    merged_df.drop(columns="_merge", inplace=True)

    return merged_df


def merge_mapping_data_with_practice_level_data(
    practice_level_data: pd.DataFrame,
    mapping_data: pd.DataFrame,
    _save_interim_output: bool = False,
) -> pd.DataFrame:
    """
    Merges the mapping data with the practice data and returns the result.

    Parameters
    ----------
    mapping_data : pd.DataFrame
        The mapping data to merge.
    practice_level_data : list[pd.DataFrame]
        The practice data to merge.
    _save_interim_output : bool, optional
        Whether to save the interim output, by default False

    Returns
    -------
    pd.DataFrame
        The merged DataFrame.
    """
    logger.info("Merging mapping data with practice data")
    merged_data = pd.merge(
        left=practice_level_data,
        right=mapping_data,
        on="GP_CODE",
        # how="left",
        indicator=True,
    ).pipe(check_merge)

    if _save_interim_output:
        save_path = INTERIM_DATA_DIR / "merged_data.csv"
        logger.info(f"Saving interim output to {save_path}")
        merged_data.to_csv(save_path, index=False)

    return merged_data
