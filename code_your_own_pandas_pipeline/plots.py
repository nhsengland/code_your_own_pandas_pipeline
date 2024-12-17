"""
This module provides function for generating and saving plots.
"""

import pandas as pd
from loguru import logger


def save_plot(plot, output_folder: str, plot_name: str) -> None:
    """
    Save the plot to the output folder.

    Parameters
    ----------
    plot : matplotlib.pyplot
        The plot to save.
    output_folder : str
        The output folder to save the plot.
    plot_name : str
        The plot name.

    Returns
    -------
    None
    """
    logger.info(f"Saving the plot {plot_name} to {output_folder}.")

    logger.warning("This function is not yet implemented.")


def plot_monthly_gp_appointments(
    monthly_gp_appointments: pd.DataFrame, output_folder: str
) -> None:
    """
    Plot the monthly GP appointments.

    Parameters
    ----------
    monthly_gp_appointments : pd.DataFrame
        The monthly GP appointments data.
    output_folder : str
        The output folder to save the plots.

    Returns
    -------
    None
    """
    logger.info("Plotting the monthly GP appointments.")

    logger.warning("This function is not yet implemented.")


def plot_monthly_region_appointments(
    monthly_region_appointments: pd.DataFrame, output_folder: str
) -> None:
    """
    Plot the monthly region appointments.

    Parameters
    ----------
    monthly_region_appointments : pd.DataFrame
        The monthly region appointments data.
    output_folder : str
        The output folder to save the plots.

    Returns
    -------
    None
    """
    logger.info("Plotting the monthly region appointments.")

    logger.warning("This function is not yet implemented.")
