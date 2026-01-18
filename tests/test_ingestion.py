import os
import sys
from pathlib import Path

# Setup paths
root = Path(__file__).parent.parent
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

import pandas as pd
import pytest
from src.ingestion import ingest_all_assets

TEST_ASSETS = ["AAPL", "SPY"]

@pytest.fixture
def patched_ingestion(tmp_path, monkeypatch):
    """Patch BRONZE_DIR and load_assets to use test assets and temporary folder."""
    test_dir = tmp_path / "bronze_test"
    test_dir.mkdir()
    
    # Force the code to use the temp directory for output
    monkeypatch.setattr("src.ingestion.BRONZE_DIR", str(test_dir))
    # Mock load_assets so we only test 2 assets instead of your full list
    monkeypatch.setattr("src.ingestion.load_assets", lambda: TEST_ASSETS)
    return test_dir

def test_bronze_files_created(patched_ingestion):
    # Use a specific date range to ensure API response is consistent
    files = ingest_all_assets(start_date="2024-01-01", end_date="2024-01-05")
    assert len(files) == len(TEST_ASSETS)
    for f in files:
        f_path = Path(f)
        assert f_path.exists()
        assert f_path.stat().st_size > 0

def test_csv_columns(patched_ingestion):
    files = ingest_all_assets(start_date="2024-01-01", end_date="2024-01-05")
    # Note: column names from yfinance are often capitalized
    expected_columns = {"Date", "Open", "High", "Low", "Close", "Volume", "symbol"}
    for f in files:
        df = pd.read_csv(f)
        assert expected_columns.issubset(set(df.columns))

def test_date_range(patched_ingestion):
    files = ingest_all_assets(start_date="2024-01-01", end_date="2024-01-05")
    for f in files:
        df = pd.read_csv(f)
        df['Date'] = pd.to_datetime(df['Date'])
        min_date, max_date = df['Date'].min(), df['Date'].max()
        assert min_date >= pd.Timestamp("2024-01-01")
        assert max_date <= pd.Timestamp("2024-01-05")