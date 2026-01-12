from pathlib import Path
import pandas as pd
import pytest
from src.ingestion import ingest_all_assets, BRONZE_DIR

TEST_ASSETS = ["AAPL", "SPY"]

# ----------------------------
# 1️⃣ Fixture: setup test directory and patch ingestion
# ----------------------------

@pytest.fixture
def patched_ingestion(tmp_path, monkeypatch):
    """Patch BRONZE_DIR and load_assets to use test assets and temporary folder."""
    test_dir = tmp_path / "bronze_test"
    test_dir.mkdir()
    monkeypatch.setattr("src.ingestion.BRONZE_DIR", test_dir)
    monkeypatch.setattr("src.ingestion.load_assets", lambda: TEST_ASSETS)
    return test_dir

# ----------------------------
# 2️⃣ Test: Files are created
# ----------------------------

def test_bronze_files_created(patched_ingestion):
    files = ingest_all_assets(start_date="2020-01-01", end_date="2020-01-05")
    assert len(files) == len(TEST_ASSETS)
    for f in files:
        f_path = Path(f)  # convert string to Path
        assert f_path.exists()         # file exists
        assert f_path.stat().st_size > 0  # file is not empty

# ----------------------------
# 3️⃣ Test: CSV Columns
# ----------------------------

def test_csv_columns(patched_ingestion):
    files = ingest_all_assets(start_date="2020-01-01", end_date="2020-01-05")
    expected_columns = {"Date", "Open", "High", "Low", "Close", "Volume", "symbol"}
    for f in files:
        df = pd.read_csv(f)
        assert expected_columns.issubset(df.columns)

# ----------------------------
# 4️⃣ Test: Date Range
# ----------------------------

def test_date_range(patched_ingestion):
    files = ingest_all_assets(start_date="2020-01-01", end_date="2020-01-05")
    for f in files:
        df = pd.read_csv(f)
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        min_date, max_date = df['Date'].min(), df['Date'].max()
        assert min_date >= pd.Timestamp("2020-01-01")
        assert max_date <= pd.Timestamp("2020-01-05")
