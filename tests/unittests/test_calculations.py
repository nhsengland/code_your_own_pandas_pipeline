"""
Tests for code_your_own_pandas_pipeline.calculations
"""

import pytest
import pandas as pd
from code_your_own_pandas_pipeline.calculations import (
    calculate_total_appointments,
    calculate_did_not_attend_rate,
    calculate_attended_rate,
    calculate_appointment_columns,
)


def test_calculate_total_appointments():
    """Test calculate_total_appointments function"""
    data = {"ATTENDED": [10, 20, 30], "DID_NOT_ATTEND": [1, 2, 3], "UNKNOWN": [0, 1, 0]}
    df = pd.DataFrame(data)
    result = calculate_total_appointments(df)
    expected_total = [11, 23, 33]
    assert result["TOTAL_APPOINTMENTS"].tolist() == expected_total


def test_calculate_did_not_attend_rate():
    """Test calculate_did_not_attend_rate function"""
    data = {
        "ATTENDED": [10, 20, 30],
        "DID_NOT_ATTEND": [1, 2, 3],
        "TOTAL_APPOINTMENTS": [11, 22, 33],
    }
    df = pd.DataFrame(data)
    result = calculate_did_not_attend_rate(df)
    expected_rate = [1 / 11, 2 / 22, 3 / 33]
    assert result["DID_NOT_ATTEND_RATE"].tolist() == expected_rate


def test_calculate_attended_rate():
    """Test calculate_attended_rate function"""
    data = {
        "ATTENDED": [10, 20, 30],
        "DID_NOT_ATTEND": [1, 2, 3],
        "TOTAL_APPOINTMENTS": [11, 22, 33],
    }
    df = pd.DataFrame(data)
    result = calculate_attended_rate(df)
    expected_rate = [10 / 11, 20 / 22, 30 / 33]
    assert result["ATTENDED_RATE"].tolist() == expected_rate


@pytest.mark.parametrize(
    "data, expected_total, expected_dna_rate, expected_attended_rate",
    [
        (
            {"ATTENDED": [10, 20, 30], "DID_NOT_ATTEND": [1, 2, 3]},
            [11, 22, 33],
            [1 / 11, 2 / 22, 3 / 33],
            [10 / 11, 20 / 22, 30 / 33],
        )
    ],
)
def test_calculate_appointment_columns(
    mocker, data, expected_total, expected_dna_rate, expected_attended_rate
):
    """Test calculate_appointment_columns function with mocks"""
    df = pd.DataFrame(data)
    
    mock_calculate_total_appointments = mocker.patch(
        "code_your_own_pandas_pipeline.calculations.calculate_total_appointments",
        return_value=df.assign(TOTAL_APPOINTMENTS=expected_total),
    )
    mock_calculate_did_not_attend_rate = mocker.patch(
        "code_your_own_pandas_pipeline.calculations.calculate_did_not_attend_rate",
        return_value=df.assign(DID_NOT_ATTEND_RATE=expected_dna_rate),
    )
    mock_calculate_attended_rate = mocker.patch(
        "code_your_own_pandas_pipeline.calculations.calculate_attended_rate",
        return_value=df.assign(ATTENDED_RATE=expected_attended_rate),
    )

    result = calculate_appointment_columns(df)

    mock_calculate_total_appointments.assert_called_once_with(df)
    mock_calculate_did_not_attend_rate.assert_called_once_with(df)
    mock_calculate_attended_rate.assert_called_once_with(df)

    assert result["TOTAL_APPOINTMENTS"].tolist() == expected_total
    assert result["DID_NOT_ATTEND_RATE"].tolist() == expected_dna_rate
    assert result["ATTENDED_RATE"].tolist() == expected_attended_rate
