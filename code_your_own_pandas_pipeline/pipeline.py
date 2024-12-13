"""
_summary_
"""

from pathlib import Path

from loguru import logger

from code_your_own_pandas_pipeline import aggregations as agg
from code_your_own_pandas_pipeline import data_in, plots, processing, utils


@utils.timeit
def main(_save_interim_output: bool = False) -> None:
    """
    Main function to run the GP Appointment Data Pipeline.

    Parameters
    ----------
    _save_interim_output : bool, optional
        Flag to determine whether to save interim output files, by default True.

    Returns
    -------
    None
    """
    logger.level("START", no=15, color="<green><bold>")
    logger.log("START", "Starting the GP Appointment Data Pipeline")

    data_in.download_and_extract_zip(
        "https://files.digital.nhs.uk/A5/B4AB19/Practice_Level_Crosstab_Sep_24.zip"
    )

    mapping_data = data_in.read_mapping_data()

    practice_level_data = (
        data_in.read_practice_level_data()
        .pipe(processing.tidy_practice_level_data, _save_interim_output)
        .pipe(
            processing.merge_mapping_data_with_practice_level_data,
            mapping_data=mapping_data,
            _save_interim_output=_save_interim_output,
        )
    )

    practice_level_pivot = agg.pivot_practice_level_data(practice_level_data)
    summaries = agg.batch_summarize_monthly_aggregate_appointments(practice_level_pivot)

    if _save_interim_output:
        for key, value in summaries.items():
            value.to_csv(Path("data", f"{key}_summary.csv"), index=False)

    plots.batch_plot_monthly_attd_rate_by(summaries=summaries)

    logger.success("GP Appointment Data Pipeline completed")


if __name__ == "__main__":
    main()
