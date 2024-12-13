"""
This module provides functions to calculate appointment statistics for a given practice level DataFrame.

Functions
---------
calculate_total_appointments(practice_level_pivot: pd.DataFrame) -> pd.DataFrame

calculate_did_not_attend_rate(practice_level_pivot: pd.DataFrame) -> pd.DataFrame

calculate_attended_rate(practice_level_pivot: pd.DataFrame) -> pd.DataFrame

calculate_appointment_columns(practice_level_pivot: pd.DataFrame) -> pd.DataFrame

"""

from typing import Optional
from matplotlib import axis
import pandas as pd

# from loguru import logger


def calculate_total_appointments(
    practice_level_pivot: pd.DataFrame, appointment_cols: Optional[list[str]] = None
) -> pd.DataFrame:
    """
    Calculate the total number of appointments by summing attended and did not attend appointments.
    Parameters
    ----------
    practice_level_pivot : pd.DataFrame
        A DataFrame containing columns "ATTENDED" and "DID_NOT_ATTEND" representing the number of attended and missed appointments respectively.
    Returns
    -------
    pd.DataFrame
        The input DataFrame with an additional column "TOTAL_APPOINTMENTS" which is the sum of "ATTENDED" and "DID_NOT_ATTEND".
    """
    if not appointment_cols:
        appointment_cols = ["ATTENDED", "DID_NOT_ATTEND", "UNKNOWN"]

    # logger.info("Calculating total appointments")
    practice_level_pivot[appointment_cols] = practice_level_pivot[appointment_cols].fillna(
        0, inplace=False
    )

    practice_level_pivot["TOTAL_APPOINTMENTS"] = practice_level_pivot[appointment_cols].sum(axis=1)

    return practice_level_pivot


def calculate_did_not_attend_rate(practice_level_pivot) -> pd.DataFrame:
    """
    Calculate the rate of missed appointments.
    Parameters
    ----------
    practice_level_pivot : pd.DataFrame
        A DataFrame containing columns "ATTENDED" and "DID_NOT_ATTEND" representing the number of attended and missed appointments respectively.
    Returns
    -------
    pd.DataFrame
        The input DataFrame with an additional column "DID_NOT_ATTEND_RATE" which is the rate of missed appointments.
    """
    # logger.info("Calculating did not attend rate")
    practice_level_pivot["DID_NOT_ATTEND_RATE"] = (
        practice_level_pivot["DID_NOT_ATTEND"] / practice_level_pivot["TOTAL_APPOINTMENTS"]
    )

    return practice_level_pivot


def calculate_attended_rate(practice_level_pivot) -> pd.DataFrame:
    """
    Calculate the rate of attended appointments.
    Parameters
    ----------
    practice_level_pivot : pd.DataFrame
        A DataFrame containing columns "ATTENDED" and "DID_NOT_ATTEND" representing the number of attended and missed appointments respectively.
    Returns
    -------
    pd.DataFrame
        The input DataFrame with an additional column "ATTENDED_RATE" which is the rate of attended appointments.
    """
    # logger.info("Calculating attended rate")
    practice_level_pivot["ATTENDED_RATE"] = (
        practice_level_pivot["ATTENDED"] / practice_level_pivot["TOTAL_APPOINTMENTS"]
    )

    return practice_level_pivot


def calculate_appointment_columns(practice_level_pivot) -> pd.DataFrame:
    """
    Calculate the total number of appointments, the rate of missed appointments, and the rate of attended appointments.
    Parameters
    ----------
    practice_level_pivot : pd.DataFrame
        A DataFrame containing columns "ATTENDED" and "DID_NOT_ATTEND" representing the number of attended and missed appointments respectively.
    Returns
    -------
    pd.DataFrame
        The input DataFrame with additional columns "TOTAL_APPOINTMENTS", "DID_NOT_ATTEND_RATE", and "ATTENDED_RATE".
    """
    # logger.info("Calculating appointment columns")
    practice_level_pivot = (
        practice_level_pivot.pipe(calculate_total_appointments)
        .pipe(calculate_attended_rate)
        .pipe(calculate_did_not_attend_rate)
    )
    return practice_level_pivot
