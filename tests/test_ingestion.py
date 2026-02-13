from pathlib import Path
import pandas as pd
import pytest
import src.ingestion as ingestion


# Provides a standard mock yfinance DataFrame for consistent testing
@pytest.fixture
def fake_yfinance_df():
    return pd.DataFrame({
        "Date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
        "Open": [100.0, 101.0],
        "High": [105.0, 106.0],
        "Low": [99.0, 100.0],
        "Close": [104.0, 105.0],
        "Volume": [1000, 1100],
    })


# MultiIndex mock to test column flattening logic
@pytest.fixture
def fake_multiindex_df():
    columns = pd.MultiIndex.from_tuples([
        ('Close', 'AAPL'),
        ('Open', 'AAPL'),
        ('High', 'AAPL'),
        ('Low', 'AAPL'),
        ('Volume', 'AAPL'),
    ])
    data = [[150.0, 149.0, 155.0, 148.0, 1000]]
    return pd.DataFrame(data, columns=columns, index=pd.to_datetime(["2024-01-01"]))


# Verifies correct handling of MultiIndex columns
def test_ingestion_flattens_multiindex(tmp_path, monkeypatch, fake_multiindex_df):
    bronze_dir = tmp_path / "multi_test"

    monkeypatch.setattr(
        ingestion.yf,
        "download",
        lambda *args, **kwargs: fake_multiindex_df
    )

    files = ingestion.ingest_all_assets(
        tickers=["AAPL"],
        start_date="2024-01-01",
        end_date="2024-01-02",
        bronze_dir=bronze_dir
    )

    df = pd.read_csv(files[0])

    assert "Close" in df.columns
    assert "symbol" in df.columns
    assert df["symbol"].iloc[0] == "AAPL"


# Test for file naming convention
def test_ingestion_naming_convention(tmp_path, monkeypatch, fake_yfinance_df):
    bronze_dir = tmp_path / "name_test"

    monkeypatch.setattr(
        ingestion.yf,
        "download",
        lambda *args, **kwargs: fake_yfinance_df
    )

    files = ingestion.ingest_all_assets(
        tickers=["BTC-USD"],
        start_date="2024-01-01",
        end_date="2024-01-02",
        bronze_dir=bronze_dir
    )

    filename = Path(files[0]).name

    assert filename.startswith("BTC-USD_")
    assert filename.endswith(".csv")


# Tests behavior when API returns no data
def test_ingestion_handles_empty_api_response(tmp_path, monkeypatch):
    bronze_dir = tmp_path / "empty_test"

    monkeypatch.setattr(
        ingestion.yf,
        "download",
        lambda *args, **kwargs: pd.DataFrame()
    )

    files = ingestion.ingest_all_assets(
        tickers=["VOID"],
        start_date="2024-01-01",
        end_date="2024-01-05",
        bronze_dir=bronze_dir
    )

    assert len(files) == 0
