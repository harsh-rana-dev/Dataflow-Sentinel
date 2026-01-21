from datetime import date
import pandas as pd

from src.validation import validate_bronze_dataframe


def test_valid_row_passes_validation():
    """A fully valid row should appear in the silver dataframe."""
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


def test_invalid_symbol_is_rejected():
    """Empty symbol should cause row rejection."""
    df = pd.DataFrame(
        [
            {
                "symbol": "",
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
    """Valid rows must survive even if others are invalid."""
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
    assert silver_df.iloc[0]["date"] == date(2024, 1, 1)


def test_silver_output_schema():
    """Silver dataframe must have the exact expected schema."""
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
