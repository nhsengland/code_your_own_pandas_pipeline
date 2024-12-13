"""
_summary_
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
    Pivot the practice level data to summarize appointment statuses.

    Parameters:
    practice_level_data (pd.DataFrame): A DataFrame containing practice level data with columns:
        - "APPOINTMENT_MONTH_START_DATE"
        - "GP_NAME"
        - "SUPPLIER"
        - "PCN_NAME"
        - "SUB_ICB_LOCATION_NAME"
        - "ICB_NAME"
        - "REGION_NAME"
        - "HCP_TYPE"
        - "APPT_MODE"
        - "NATIONAL_CATEGORY"
        - "TIME_BETWEEN_BOOK_AND_APPT"
        - "APPT_STATUS"
        - "COUNT_OF_APPOINTMENTS"

    Returns:
    pd.DataFrame: A pivoted DataFrame with appointment statuses as columns and the count of appointments as values.
    The columns "DNA" and "Attended" are renamed to "DID_NOT_ATTEND" and "ATTENDED" respectively.
    """
    if not index:
        index = ["APPOINTMENT_MONTH_START_DATE", *AGG_COLS]
    if not rename_columns:
        rename_columns = {"DNA": "DID_NOT_ATTEND", "Attended": "ATTENDED"}

    logger.info("Pivoting practice level data")
    practice_level_pivot = practice_level_data.pivot(index=index, columns=columns, values=values)

    practice_level_pivot = practice_level_pivot.reset_index()

    practice_level_pivot = practice_level_pivot.rename(columns=rename_columns)

    return practice_level_pivot


def summarize_monthly_appointment_status(practice_level_data: pd.DataFrame) -> pd.DataFrame:
    """
    Summarizes the monthly appointment status by aggregating the count of appointments.

    Args:
        practice_level_data (pd.DataFrame): DataFrame containing appointment data with columns
            "APPOINTMENT_MONTH_START_DATE", "APPT_STATUS", and "COUNT_OF_APPOINTMENTS".

    Returns:
        pd.DataFrame: A DataFrame with the aggregated count of appointments per month and status,
            with columns "APPOINTMENT_MONTH_START_DATE", "Appointment Status", and "COUNT_OF_APPOINTMENTS".
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
    Summarizes monthly aggregate appointments from a practice-level pivot DataFrame.

    Args:
        practice_level_pivot (pd.DataFrame): The input DataFrame containing practice-level appointment data.
        agg_cols (Optional[list[str]]): A list of additional columns to group by. Defaults to None.
        add_rate_cols (bool): Whether to add rate columns to the output DataFrame. Defaults to True.

    Returns:
        pd.DataFrame: A DataFrame with monthly aggregate appointments, including attended and did not attend counts.
    """
    if not agg_cols:
        agg_cols = []

    monthly_aggregate_appointments = (
        practice_level_pivot.groupby(["APPOINTMENT_MONTH_START_DATE", *agg_cols])
        .agg({"ATTENDED": "sum", "DID_NOT_ATTEND": "sum"})
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
