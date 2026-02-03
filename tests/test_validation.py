import pytest
import pandas as pd
from datetime import date
from pathlib import Path
from src.validation import (
    validate_bronze_dataframe, 
    validate_bronze_csv, 
    save_silver_dataframe
)

# # Provides a standard valid input dictionary to reduce boilerplate in tests
@pytest.fixture
def valid_row_dict():
    return {
        "symbol": "AAPL",
        "Date": date(2024, 1, 1),
        "Open": 100.0, "High": 110.0, "Low": 95.0, "Close": 105.0,
        "Volume": 1_000_000,
    }

# # Verifies that a perfectly formatted row is correctly transformed to the silver schema
def test_valid_row_passes_validation(valid_row_dict):
    df = pd.DataFrame([valid_row_dict])
    silver_df = validate_bronze_dataframe(df)

    assert len(silver_df) == 1
    assert silver_df.iloc[0]["symbol"] == "AAPL"
    # Ensure column names were converted to lowercase (Silver contract)
    assert "open" in silver_df.columns 

# # Ensures the Pydantic Field(min_length=1) constraint is working
def test_invalid_symbol_is_rejected(valid_row_dict):
    valid_row_dict["symbol"] = ""  # Violates min_length=1
    df = pd.DataFrame([valid_row_dict])

    silver_df = validate_bronze_dataframe(df)
    assert silver_df.empty

# # Confirms that the pipeline is resilient and doesn't crash on a single bad record
def test_mixed_valid_and_invalid_rows(valid_row_dict):
    bad_row = valid_row_dict.copy()
    bad_row["Date"] = "not-a-date" # Type error for Pydantic date field
    
    df = pd.DataFrame([valid_row_dict, bad_row])
    silver_df = validate_bronze_dataframe(df)

    assert len(silver_df) == 1
    assert silver_df.iloc[0]["date"] == date(2024, 1, 1)

# # IMPROVEMENT: Test the file-to-dataframe logic using tmp_path
def test_validate_bronze_csv(tmp_path, valid_row_dict):
    # Create a dummy bronze CSV
    bronze_file = tmp_path / "test_bronze.csv"
    pd.DataFrame([valid_row_dict]).to_csv(bronze_file, index=False)
    
    silver_df = validate_bronze_csv(bronze_file)
    assert not silver_df.empty
    assert "symbol" in silver_df.columns

# # IMPROVEMENT: Test the save logic and directory creation
def test_save_silver_dataframe(tmp_path):
    silver_dir = tmp_path / "silver_output"
    source_file = Path("AAPL_raw.csv")
    df = pd.DataFrame([{"symbol": "AAPL", "close": 150.0}])
    
    output_path = save_silver_dataframe(df, source_file, silver_dir)
    
    assert output_path.exists()
    assert silver_dir.is_dir()
    assert "AAPL_raw_silver" in output_path.name

# # IMPROVEMENT: Test for missing columns (KeyError handling)
def test_validation_handles_missing_columns():
    # DataFrame missing the "Volume" column required by validate_bronze_dataframe
    df = pd.DataFrame([{"symbol": "AAPL", "Date": date(2024,1,1)}])
    
    # Should not crash, but return empty DF because of the try-except block
    silver_df = validate_bronze_dataframe(df)
    assert silver_df.empty