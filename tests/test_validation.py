import pandas as pd
from datetime import date

from src.validation import validate_bronze_dataframe


def test_valid_rows_pass_validation():
    """Valid rows should pass and appear in silver output."""

    df = pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "Date": date(2024, 1, 1),
                "Open": 100.0,
                "High": 110.0,
                "Low": 95.0,
                "Close": 105.0,
                "Volume": 1_000_000,
            }
        ]
    )

    silver_df = validate_bronze_dataframe(df)

    assert len(silver_df) == 1
    assert silver_df.iloc[0]["symbol"] == "AAPL"


def test_invalid_row_is_rejected():
    """Row with missing required fields should be rejected."""

    df = pd.DataFrame(
        [
            {
                "symbol": "",  # invalid (empty)
                "Date": date(2024, 1, 1),
                "Open": 100.0,
                "High": 110.0,
                "Low": 95.0,
                "Close": 105.0,
                "Volume": 1_000_000,
            }
        ]
    )

    silver_df = validate_bronze_dataframe(df)

    assert silver_df.empty


def test_mixed_valid_and_invalid_rows():
    """Valid rows should survive even if some rows are invalid."""

    df = pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "Date": date(2024, 1, 1),
                "Open": 100.0,
                "High": 110.0,
                "Low": 95.0,
                "Close": 105.0,
                "Volume": 1_000_000,
            },
            {
                "symbol": "AAPL",
                "Date": "not-a-date",  # invalid
                "Open": 100.0,
                "High": 110.0,
                "Low": 95.0,
                "Close": 105.0,
                "Volume": 1_000_000,
            },
        ]
    )

    silver_df = validate_bronze_dataframe(df)

    assert len(silver_df) == 1


def test_output_schema_is_exact():
    """Silver dataframe must have the expected columns."""

    df = pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "Date": date(2024, 1, 1),
                "Open": 100.0,
                "High": 110.0,
                "Low": 95.0,
                "Close": 105.0,
                "Volume": 1_000_000,
            }
        ]
    )

    silver_df = validate_bronze_dataframe(df)

    expected_columns = {
        "symbol",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
    }

    assert set(silver_df.columns) == expected_columns
