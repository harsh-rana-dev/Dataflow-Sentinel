import os
import shutil
import pandas as pd
import pytest
from src.ingestion import ingest_all_assets, BRONZE_DIR, load_assets

BRONZE_TEST_DIR = "data/bronze_test"

# ----------------------------
# 1️⃣ Setup and Teardown
# ----------------------------

@pytest.fixture(scope="module", autouse=True)
def cleanup():
    # Remove test folder if exists before/after tests
    if os.path.exists(BRONZE_TEST_DIR):
        shutil.rmtree(BRONZE_TEST_DIR)
    os.makedirs(BRONZE_TEST_DIR)
    yield
    if os.path.exists(BRONZE_TEST_DIR):
        shutil.rmtree(BRONZE_TEST_DIR)

# ----------------------------
# 2️⃣ Test File Creation
# ----------------------------

def test_bronze_files_created(monkeypatch):
    # Patch BRONZE_DIR to use test folder
    monkeypatch.setattr("src.ingestion.BRONZE_DIR", BRONZE_TEST_DIR)

    # Monkeypatch load_assets to only use test assets
    test_assets = ["AAPL", "SPY"]
    monkeypatch.setattr("src.ingestion.load_assets", lambda: test_assets)

    files = ingest_all_assets(start_date="2020-01-01", end_date="2020-01-05")
    assert len(files) == len(test_assets)
    for f in files:
        assert os.path.exists(f)

# ----------------------------
# 3️⃣ Test CSV Columns
# ----------------------------

def test_csv_columns(monkeypatch):
    monkeypatch.setattr("src.ingestion.BRONZE_DIR", BRONZE_TEST_DIR)

    # Only use test assets
    test_assets = ["AAPL", "SPY"]
    monkeypatch.setattr("src.ingestion.load_assets", lambda: test_assets)

    files = ingest_all_assets(start_date="2020-01-01", end_date="2020-01-05")
    expected_columns = ["Date", "Open", "High", "Low", "Close", "Volume", "symbol"]

    for f in files:
        df = pd.read_csv(f)
        # Only assert that expected columns exist (ignore extra columns/order)
        assert set(expected_columns).issubset(df.columns)

# ----------------------------
# 4️⃣ Test Date Range
# ----------------------------

def test_date_range(monkeypatch):
    monkeypatch.setattr("src.ingestion.BRONZE_DIR", BRONZE_TEST_DIR)

    # Only use test assets
    test_assets = ["AAPL", "SPY"]
    monkeypatch.setattr("src.ingestion.load_assets", lambda: test_assets)

    files = ingest_all_assets(start_date="2020-01-01", end_date="2020-01-05")
    for f in files:
        df = pd.read_csv(f)
        # Convert Date column to datetime
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        min_date = df['Date'].min()
        max_date = df['Date'].max()
        assert min_date >= pd.Timestamp("2020-01-01")
        assert max_date <= pd.Timestamp("2020-01-05")
