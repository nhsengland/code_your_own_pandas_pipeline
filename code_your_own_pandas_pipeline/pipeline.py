"""
Main pipeline for the code_your_own_pandas_pipeline package.
"""

import pandas as pd
from loguru import logger

from code_your_own_pandas_pipeline import aggregations, data_in, plots, processing

placeholder_df = pd.DataFrame()


def main() -> None:
    """
    Main function to run the pipeline.

    Returns
    -------
    None
    """
    logger.level("START", no=15, color="<green><bold>")
    logger.log("START", "Starting the GP Appointment Data Pipeline")

    data_in.read_mapping_data()
    data_in.read_practice_crosstab_data()

    processing.tidy_practice_level_data(placeholder_df)
    processing.merge_mapping_and_practice_data(placeholder_df, placeholder_df)

    aggregations.pivot_practice_level_data(placeholder_df)
    aggregations.summarize_monthly_gp_appointments(placeholder_df)
    aggregations.summarize_monthly_region_appointments(placeholder_df)

    plots.plot_monthly_gp_appointments(placeholder_df, "placeholder_str")
    plots.plot_monthly_region_appointments(placeholder_df, "placeholder_str")

    logger.success("GP Appointment Data Pipeline Completed")


if __name__ == "__main__":
    main()
