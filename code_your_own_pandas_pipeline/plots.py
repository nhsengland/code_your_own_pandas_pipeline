"""
This module provides functions for generating and saving plots of monthly attendance rates using seaborn and pandas.

Functions
---------
point_monthly_attd_rate_by(summary_df: pd.DataFrame, agg_by: str) -> FacetGrid

violin_monthly_attd_rate_by(summary_df: pd.DataFrame, agg_by: str) -> FacetGrid
    Generates a violin plot of monthly attendance rates, aggregated by a specified column.

save_monthly_attendance_plot(plot: FacetGrid, col_name: str, figures_path: Optional[Path] = None)
    Saves a monthly attendance plot to a specified directory.

batch_plot_monthly_attd_rate_by(summaries: Dict[str, pd.DataFrame]) -> None
    Generates and saves monthly attendance rate plots for each summary DataFrame in the given dictionary.
"""

from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import seaborn as sns
from loguru import logger
from seaborn.axisgrid import FacetGrid
from tqdm import tqdm

from code_your_own_pandas_pipeline.config import FIGURES_DIR


def point_monthly_attd_rate_by(summary_df: pd.DataFrame, agg_by: str) -> FacetGrid:
    """
    Generates a point plot of monthly attendance rates, aggregated by a specified column.
    Parameters
    ----------
    summary_df : pd.DataFrame
        DataFrame containing the summary data with columns 'APPOINTMENT_MONTH_START_DATE' and 'ATTENDED_RATE'.
    agg_by : str
        The column name by which to aggregate the data for the plot.
    Returns
    -------
    FacetGrid
        A seaborn FacetGrid object representing the point plot.
    Raises
    ------
    AssertionError
        If the number of unique categories in the aggregation column exceeds 20.
    """

    assert summary_df[agg_by].nunique() <= 20, "Too many unique categories for display"

    plt = sns.catplot(
        x="APPOINTMENT_MONTH_START_DATE",
        y="ATTENDED_RATE",
        hue=agg_by,
        data=summary_df,
        aspect=2,
        kind="point",
    )

    plt.set(
        title=f"Monthly Attendance Rate by {agg_by}",
        xlabel="Month Start Date",
        ylabel="Attendance Rate",
        ylim=(0.85, 1.0),
    )

    return plt


def violin_monthly_attd_rate_by(summary_df: pd.DataFrame, agg_by: str) -> FacetGrid:
    """
    Generates a point plot of monthly attendance rates, aggregated by a specified column.
    Parameters
    ----------
    summary_df : pd.DataFrame
        DataFrame containing the summary data with columns 'APPOINTMENT_MONTH_START_DATE' and 'ATTENDED_RATE'.
    agg_by : str
        The column name by which to aggregate the data for the plot.
    Returns
    -------
    FacetGrid
        A seaborn FacetGrid object representing the point plot.
    Raises
    ------
    AssertionError
        If the number of unique categories in the aggregation column exceeds 20.
    """

    assert summary_df[agg_by].nunique() > 20, "Too few unique categories for display"

    plt = sns.catplot(
        x="APPOINTMENT_MONTH_START_DATE",
        y="ATTENDED_RATE",
        data=summary_df,
        aspect=2,
        kind="violin",
    )

    plt.set(
        title=f"Monthly Attendance Rate by {agg_by}",
        xlabel="Month Start Date",
        ylabel="Attendance Rate",
        # ylim=(0.85, 1.0),
    )

    return plt


def save_monthly_attendance_plot(
    plot: FacetGrid, col_name: str, figures_path: Optional[Path] = None
):
    """
    Save a monthly attendance plot to a specified directory.
    Parameters
    ----------
    plot : FacetGrid
        The plot object to be saved.
    col_name : str
        The name of the column used for the plot.
    figures_path : Optional[Path], default=None
        The directory path where the plot will be saved. If None, a default directory is used.
    Returns
    -------
    None
    """

    if figures_path is None:
        figures_path = FIGURES_DIR

    file_path = figures_path / f"monthly_attendance_rate_by_{col_name}.png"
    plot.savefig(file_path)

    logger.info(f"Saved plot to {file_path.relative_to(Path.cwd())}")


def batch_plot_monthly_attd_rate_by(summaries: Dict[str, pd.DataFrame]) -> None:
    """
    Generate and save monthly attendance rate plots for each summary DataFrame in the given dictionary.

    For each key-value pair in the `summaries` dictionary, this function generates a plot of the monthly attendance rate.
    If the number of unique values in the aggregation column is less than 20, a violin plot is generated.
    Otherwise, a point plot is generated. The resulting plot is then saved.

    Parameters
    ----------
    summaries : Dict[str, pd.DataFrame]
        A dictionary where the keys are column names to aggregate by, and the values are summary DataFrames containing
        the data to be plotted.

    Returns
    -------
    None
    """
    logger.info("Batch plotting monthly attendance rate by aggregation column")
    for agg_column, summary_df in tqdm(summaries.items(), desc="Plotting summaries"):
        if summary_df[agg_column].nunique() > 20:
            monthly_attendance_rate_by_agg_plt = violin_monthly_attd_rate_by(
                agg_by=agg_column, summary_df=summary_df
            )
        else:
            monthly_attendance_rate_by_agg_plt = point_monthly_attd_rate_by(
                agg_by=agg_column, summary_df=summary_df
            )

        save_monthly_attendance_plot(monthly_attendance_rate_by_agg_plt, agg_column)
