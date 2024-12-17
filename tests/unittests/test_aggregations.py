"""
Tests for code_your_own_pandas_pipeline.aggregations
"""

import pandas as pd
import pytest

from code_your_own_pandas_pipeline.aggregations import (
    pivot_practice_level_data,
    summarize_monthly_gp_appointments,
    summarize_monthly_region_appointments,
)


@pytest.fixture
def practice_test_data():
    return pd.DataFrame(
        columns=[
            "APPOINTMENT_MONTH_START_DATE",
            "GP_NAME",
            "REGION_NAME",
            "APPT_STATUS",
            "COUNT_OF_APPOINTMENTS",
        ],
        data=[
            ["2021-01-01", "Example GP A", "REGION1", "ATTENDED", 1],
            ["2021-01-01", "Example GP B", "REGION1", "ATTENDED", 4],
            ["2021-02-01", "Example GP A", "REGION1", "ATTENDED", 7],
            ["2021-02-01", "Example GP B", "REGION1", "ATTENDED", 10],
            ["2021-01-01", "Example GP A", "REGION2", "ATTENDED", 1],
            ["2021-01-01", "Example GP B", "REGION2", "ATTENDED", 4],
            ["2021-02-01", "Example GP A", "REGION2", "ATTENDED", 7],
            ["2021-02-01", "Example GP B", "REGION2", "ATTENDED", 10],
            ["2021-01-01", "Example GP A", "REGION1", "ATTENDED", 1],
            ["2021-01-01", "Example GP B", "REGION1", "ATTENDED", 4],
            ["2021-02-01", "Example GP A", "REGION1", "ATTENDED", 7],
            ["2021-02-01", "Example GP B", "REGION1", "ATTENDED", 10],
            ["2021-01-01", "Example GP A", "REGION2", "ATTENDED", 1],
            ["2021-01-01", "Example GP B", "REGION2", "ATTENDED", 4],
            ["2021-02-01", "Example GP A", "REGION2", "ATTENDED", 7],
            ["2021-02-01", "Example GP B", "REGION2", "ATTENDED", 10],
            ["2021-01-01", "Example GP A", "REGION1", "DID NOT ATTEND", 2],
            ["2021-01-01", "Example GP B", "REGION1", "DID NOT ATTEND", 5],
            ["2021-02-01", "Example GP A", "REGION1", "DID NOT ATTEND", 8],
            ["2021-02-01", "Example GP B", "REGION1", "DID NOT ATTEND", 11],
            ["2021-01-01", "Example GP A", "REGION2", "DID NOT ATTEND", 2],
            ["2021-01-01", "Example GP B", "REGION2", "DID NOT ATTEND", 5],
            ["2021-02-01", "Example GP A", "REGION2", "DID NOT ATTEND", 8],
            ["2021-02-01", "Example GP B", "REGION2", "DID NOT ATTEND", 11],
            ["2021-01-01", "Example GP A", "REGION1", "DID NOT ATTEND", 2],
            ["2021-01-01", "Example GP B", "REGION1", "DID NOT ATTEND", 5],
            ["2021-02-01", "Example GP A", "REGION1", "DID NOT ATTEND", 8],
            ["2021-02-01", "Example GP B", "REGION1", "DID NOT ATTEND", 11],
            ["2021-01-01", "Example GP A", "REGION2", "DID NOT ATTEND", 2],
            ["2021-01-01", "Example GP B", "REGION2", "DID NOT ATTEND", 5],
            ["2021-02-01", "Example GP A", "REGION2", "DID NOT ATTEND", 8],
            ["2021-02-01", "Example GP B", "REGION2", "DID NOT ATTEND", 11],
            ["2021-01-01", "Example GP A", "REGION1", "UNKNOWN", 3],
            ["2021-01-01", "Example GP B", "REGION1", "UNKNOWN", 6],
            ["2021-02-01", "Example GP A", "REGION1", "UNKNOWN", 9],
            ["2021-02-01", "Example GP B", "REGION1", "UNKNOWN", 12],
            ["2021-01-01", "Example GP A", "REGION2", "UNKNOWN", 3],
            ["2021-01-01", "Example GP B", "REGION2", "UNKNOWN", 6],
            ["2021-02-01", "Example GP A", "REGION2", "UNKNOWN", 9],
            ["2021-02-01", "Example GP B", "REGION2", "UNKNOWN", 12],
            ["2021-01-01", "Example GP A", "REGION1", "UNKNOWN", 3],
            ["2021-01-01", "Example GP B", "REGION1", "UNKNOWN", 6],
            ["2021-02-01", "Example GP A", "REGION1", "UNKNOWN", 9],
            ["2021-02-01", "Example GP B", "REGION1", "UNKNOWN", 12],
            ["2021-01-01", "Example GP A", "REGION2", "UNKNOWN", 3],
            ["2021-01-01", "Example GP B", "REGION2", "UNKNOWN", 6],
            ["2021-02-01", "Example GP A", "REGION2", "UNKNOWN", 9],
            ["2021-02-01", "Example GP B", "REGION2", "UNKNOWN", 12],
        ],
    )


@pytest.fixture
def practice_pivot_test_data():
    return pd.DataFrame(
        columns=[
            "APPOINTMENT_MONTH_START_DATE",
            "GP_NAME",
            "REGION_NAME",
            "ATTENDED",
            "DID NOT ATTEND",
            "UNKNOWN",
        ],
        data=[
            ["2021-01-01", "Example GP A", "REGION1", 1, 2, 3],
            ["2021-01-01", "Example GP B", "REGION1", 4, 5, 6],
            ["2021-02-01", "Example GP A", "REGION1", 7, 8, 9],
            ["2021-02-01", "Example GP B", "REGION1", 10, 11, 12],
            ["2021-01-01", "Example GP A", "REGION2", 1, 2, 3],
            ["2021-01-01", "Example GP B", "REGION2", 4, 5, 6],
            ["2021-02-01", "Example GP A", "REGION2", 7, 8, 9],
            ["2021-02-01", "Example GP B", "REGION2", 10, 11, 12],
        ]
        * 2,
    )


class TestPivotPracticeLevelData:
    """
    Tests for the pivot_practice_level_data function.
    """

    def test_returns_dataframe(self, practice_test_data):
        """
        Check that the function returns a DataFrame.
        """
        actual = pivot_practice_level_data(practice_test_data)
        assert isinstance(actual, pd.DataFrame)

    def test_return_not_empty(self, practice_test_data):
        """
        Check that the function returns a non-empty DataFrame.
        """
        actual = pivot_practice_level_data(practice_test_data)
        assert not actual.empty

    def test_return_pivoted_data(self, practice_test_data):
        """
        Check that the function returns the pivoted data.
        """
        actual = pivot_practice_level_data(practice_test_data, practice_pivot_test_data)
        expected = practice_pivot_test_data
        assert actual.assert_frame_equal(expected)


class TestSummarizeMonthlyGPAppointments:
    """
    Tests for the summarize_monthly_gp_appointments function.
    """

    def test_returns_dataframe(self, practice_pivot_test_data):
        """
        Check that the function returns a DataFrame.
        """
        actual = summarize_monthly_gp_appointments(practice_pivot_test_data)
        assert isinstance(actual, pd.DataFrame)

    def test_return_not_empty(self, practice_pivot_test_data):
        """
        Check that the function returns a non-empty DataFrame.
        """
        actual = summarize_monthly_gp_appointments(practice_pivot_test_data)
        assert not actual.empty

    def test_return_summarized_data(self, practice_pivot_test_data):
        """
        Check that the function returns the summarized data.
        """
        actual = summarize_monthly_gp_appointments(practice_pivot_test_data)
        expected = pd.DataFrame(
            [
                ["2021-01-01", "Example GP A", 4, 8, 12],
                ["2021-01-01", "Example GP B", 16, 20, 24],
                ["2021-02-01", "Example GP A", 28, 32, 36],
                ["2021-02-01", "Example GP B", 40, 44, 48],
            ],
            columns=[
                "APPOINTMENT_MONTH_START_DATE",
                "GP_NAME",
                "ATTENDED",
                "DID NOT ATTEND",
                "UNKNOWN",
            ],
        )
        assert actual.assert_frame_equal(expected)


class TestSummarizeMonthlyRegionAppointments:
    """
    Tests for the summarize_monthly_region_appointments function.
    """

    def test_returns_dataframe(self, practice_pivot_test_data):
        """
        Check that the function returns a DataFrame.
        """
        actual = summarize_monthly_region_appointments(practice_pivot_test_data)
        assert isinstance(actual, pd.DataFrame)

    def test_return_not_empty(self, practice_pivot_test_data):
        """
        Check that the function returns a non-empty DataFrame.
        """
        actual = summarize_monthly_region_appointments(practice_pivot_test_data)
        assert not actual.empty

    def test_return_summarized_data(self, practice_pivot_test_data):
        """
        Check that the function returns the summarized data.
        """
        actual = summarize_monthly_region_appointments(practice_pivot_test_data)
        expected = pd.DataFrame(
            [
                ["2021-01-01", "Example GP A", 4, 8, 12],
                ["2021-01-01", "Example GP B", 16, 20, 24],
                ["2021-02-01", "Example GP A", 28, 32, 36],
                ["2021-02-01", "Example GP B", 40, 44, 48],
            ],
            columns=[
                "APPOINTMENT_MONTH_START_DATE",
                "GP_NAME",
                "ATTENDED",
                "DID NOT ATTEND",
                "UNKNOWN",
            ],
        )
        assert actual.assert_frame_equal(expected)
