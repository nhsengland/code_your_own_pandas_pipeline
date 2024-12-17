"""
This module provides functions to pivot and summarize practice-level appointment data.

Functions:
    pivot_practice_level_data(practice_level_data: pd.DataFrame, index: Optional[list[str]] = None, columns="APPT_STATUS", values="COUNT_OF_APPOINTMENTS", rename_columns: Optional[dict[str, str]] = None) -> pd.DataFrame:

    summarize_monthly_appointment_status(practice_level_data: pd.DataFrame) -> pd.DataFrame:

    summarize_monthly_aggregate_appointments(practice_level_pivot: pd.DataFrame, agg_cols: Optional[list[str]] = None, add_rate_cols: bool = True) -> pd.DataFrame:

    batch_summarize_monthly_aggregate_appointments(practice_level_pivot: pd.DataFrame, agg_cols: Optional[list[str]] = None, add_rate_cols: bool = True) -> Dict[str, pd.DataFrame]:
"""

from typing import Dict, Optional

import pandas as pd
from loguru import logger
from tqdm import tqdm

from code_your_own_pandas_pipeline.calculations import calculate_appointment_columns

AGG_COLS = [
    "GP_NAME",
    "SUPPLIER",
    "PCN_NAME",
    "SUB_ICB_LOCATION_NAME",
    "ICB_NAME",
    "REGION_NAME",
    "HCP_TYPE",
    "APPT_MODE",
    "NATIONAL_CATEGORY",
    "TIME_BETWEEN_BOOK_AND_APPT",
]


def pivot_practice_level_data(
    practice_level_data: pd.DataFrame,
    index: Optional[list[str]] = None,
    columns="APPT_STATUS",
    values="COUNT_OF_APPOINTMENTS",
    rename_columns: Optional[dict[str, str]] = None,
) -> pd.DataFrame:
    """
    Pivot the practice level data.

    Parameters
    ----------
    practice_level_data : pd.DataFrame
        The DataFrame containing the practice level data.
    index : list of str, optional
        The columns to use as index for the pivot table. If None, defaults to [
            "APPOINTMENT_MONTH_START_DATE",
            "GP_NAME",
            "SUPPLIER",
            "PCN_NAME",
            "SUB_ICB_LOCATION_NAME",
            "ICB_NAME",
            "REGION_NAME",
            "HCP_TYPE",
            "APPT_MODE",
            "NATIONAL_CATEGORY",
            "TIME_BETWEEN_BOOK_AND_APPT"
        ].
    columns : str, optional
        The column to use for the pivot table columns, by default "APPT_STATUS".
    values : str, optional
        The column to use for the pivot table values, by default "COUNT_OF_APPOINTMENTS".
    rename_columns : dict of str, str, optional
        Dictionary to rename columns, by default None.

    Returns
    -------
    pd.DataFrame
        The pivoted DataFrame.
    """
    if not index:
        index = ["APPOINTMENT_MONTH_START_DATE", *AGG_COLS]
    if not rename_columns:
        rename_columns = {"DNA": "DID_NOT_ATTEND", "Attended": "ATTENDED", "Unknown": "UNKNOWN"}

    logger.info("Pivoting practice level data")
    practice_level_pivot = practice_level_data.pivot(index=index, columns=columns, values=values)

    practice_level_pivot = practice_level_pivot.reset_index()

    practice_level_pivot = practice_level_pivot.rename(columns=rename_columns)

    return practice_level_pivot


def summarize_monthly_appointment_status(practice_level_data: pd.DataFrame) -> pd.DataFrame:
    """
    Summarize the monthly appointment status.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame containing the appointment data.
    date_column : str
        The name of the column containing the date information.
    status_column : str
        The name of the column containing the appointment status.

    Returns
    -------
    pd.DataFrame
        A DataFrame summarizing the count of each appointment status per month.
    """
    logger.info("Summarizing monthly appointment status")
    month_and_status_appointments = (
        practice_level_data.groupby(["APPOINTMENT_MONTH_START_DATE", "APPT_STATUS"])
        .agg({"COUNT_OF_APPOINTMENTS": "sum"})
        .reset_index()
        .rename(columns={"APPT_STATUS": "Appointment Status"})
    )

    return month_and_status_appointments


def summarize_monthly_aggregate_appointments(
    practice_level_pivot: pd.DataFrame,
    agg_cols: Optional[list[str]] = None,
    add_rate_cols: bool = True,
) -> pd.DataFrame:
    """
    Summarize the monthly aggregate appointments.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame containing the appointment data.
    date_column : str
        The name of the column containing the date information.
    agg_column : str
        The name of the column to aggregate.
    aggfunc : str or function, default 'sum'
        Aggregation function to apply to the agg_column.

    Returns
    -------
    pd.DataFrame
        A DataFrame summarizing the aggregated values per month.
    """
    if not agg_cols:
        agg_cols = []

    monthly_aggregate_appointments = (
        practice_level_pivot.groupby(["APPOINTMENT_MONTH_START_DATE", *agg_cols])
        .agg({"ATTENDED": "sum", "DID_NOT_ATTEND": "sum", "UNKNOWN": "sum"})
        .reset_index()
    )

    if add_rate_cols:
        monthly_aggregate_appointments = calculate_appointment_columns(
            monthly_aggregate_appointments
        )

    return monthly_aggregate_appointments


def batch_summarize_monthly_aggregate_appointments(
    practice_level_pivot: pd.DataFrame,
    agg_cols: Optional[list[str]] = None,
    add_rate_cols: bool = True,
) -> Dict[str, pd.DataFrame]:
    """
    Batch summarize monthly aggregate appointments.

    Parameters
    ----------
    practice_level_pivot : pd.DataFrame
        DataFrame containing practice level pivot data.
    agg_cols : list of str, optional
        List of columns to aggregate. If None, defaults to AGG_COLS.
    add_rate_cols : bool, optional
        Whether to add rate columns to the summary DataFrame, by default True.

    Returns
    -------
    Dict[str, pd.DataFrame]
        Dictionary where keys are the aggregation columns and values are the
        summarized DataFrames for each aggregation column.
    """
    if agg_cols is None:
        agg_cols = AGG_COLS

    monthly_aggregate_appointments = {}
    logger.info("Batch summarizing monthly aggregate appointments")
    tqdm_agg_cols = tqdm(agg_cols)
    for agg_col in tqdm_agg_cols:
        tqdm_agg_cols.set_description_str(f"Creating monthly appointment summaries for {agg_col}")
        summary_df = summarize_monthly_aggregate_appointments(
            practice_level_pivot, [agg_col], add_rate_cols
        )
        monthly_aggregate_appointments[agg_col] = summary_df

    return monthly_aggregate_appointments
