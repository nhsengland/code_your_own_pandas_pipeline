"""
Tests for code_your_own_pandas_pipeline.data_in
"""
import pandas as pd
import numpy as np

from code_your_own_pandas_pipeline.data_in import read_mapping_data, read_practice_crosstab_data


class TestReadMappingData:
    """
    Tests for the read_mapping_data function.
    """

    def test_return_type(self):
        """
        Test that the read_mapping_data function returns a pandas DataFrame.
        """
        actual = read_mapping_data()

        assert isinstance(actual, pd.DataFrame)

    def test_return_not_empty(self):
        """
        Tests that the read_mapping_data function returns a non-empty DataFrame
        """
        actual = read_mapping_data()

        assert not actual.empty

    def test_shape(self):
        """
        Tests that the read_mapping_data function returns a DataFrame with the correct shape.
        """
        actual = read_mapping_data()

        assert actual.shape == (6241, 11)

    def test_schema(self):
        """
        Tests that the read_mapping_data function returns a DataFrame with the correct schema.
        """
        actual = read_mapping_data()

        expected_schema = pd.Series(
            {
                "GP_CODE": np.dtype("O"),
                "GP_NAME": np.dtype("O"),
                "SUPPLIER": np.dtype("O"),
                "PCN_CODE": np.dtype("O"),
                "PCN_NAME": np.dtype("O"),
                "SUB_ICB_LOCATION_CODE": np.dtype("O"),
                "SUB_ICB_LOCATION_NAME": np.dtype("O"),
                "ICB_CODE": np.dtype("O"),
                "ICB_NAME": np.dtype("O"),
                "REGION_CODE": np.dtype("O"),
                "REGION_NAME": np.dtype("O"),
            }
        )

        assert expected_schema.assert_series_equal(actual.dtypes)


class TestReadPracticeCrosstabData:
    """
    Tests for the read_practice_crosstab_data function.
    """

    def test_return_type(self):
        """
        Test that the read_practice_crosstab_data function returns a pandas DataFrame.
        """
        actual = read_practice_crosstab_data()

        assert isinstance(actual, pd.DataFrame)

    def test_return_not_empty(self):
        """
        Tests that the read_practice_crosstab_data function returns a non-empty DataFrame
        """
        actual = read_practice_crosstab_data()

        assert not actual.empty

    def test_shape(self):
        """
        Tests that the read_practice_crosstab_data function returns a DataFrame with the correct shape.
        """
        actual = read_practice_crosstab_data()

        assert actual.shape == (2971190, 14)

    def test_schema(self):
        """
        Tests that the read_practice_crosstab_data function returns a DataFrame with the correct schema.
        """
        actual = read_practice_crosstab_data()

        expected_schema = pd.Series(
            {
                "APPOINTMENT_MONTH_START_DATE": np.dtype("O"),
                "GP_CODE": np.dtype("O"),
                "GP_NAME": np.dtype("O"),
                "SUPPLIER": np.dtype("O"),
                "PCN_CODE": np.dtype("O"),
                "PCN_NAME": np.dtype("O"),
                "SUB_ICB_LOCATION_CODE": np.dtype("O"),
                "SUB_ICB_LOCATION_NAME": np.dtype("O"),
                "HCP_TYPE": np.dtype("O"),
                "APPT_MODE": np.dtype("O"),
                "NATIONAL_CATEGORY": np.dtype("O"),
                "TIME_BETWEEN_BOOK_AND_APPT": np.dtype("O"),
                "COUNT_OF_APPOINTMENTS": np.dtype("int64"),
                "APPT_STATUS": np.dtype("O"),
            }
        )

        assert expected_schema.assert_series_equal(actual.dtypes)
