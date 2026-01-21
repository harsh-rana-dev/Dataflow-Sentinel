from pathlib import Path
import pandas as pd
import pytest

import src.ingestion as ingestion


@pytest.fixture
def fake_yfinance_df():
    return pd.DataFrame(
        {
            "Date": pd.to_datetime(
                ["2024-01-02", "2024-01-03", "2024-01-04"]
            ),
            "Open": [100.0, 101.0, 102.0],
            "High": [105.0, 106.0, 107.0],
            "Low": [99.0, 100.0, 101.0],
            "Close": [104.0, 105.0, 106.0],
            "Volume": [1000, 1100, 1200],
        }
    )


@pytest.fixture
def isolated_ingestion_env(tmp_path, monkeypatch, fake_yfinance_df):
    """
    Fully isolate ingestion from filesystem and external APIs.
    """
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()

    monkeypatch.setattr(ingestion, "BRONZE_DIR", str(bronze_dir))
    monkeypatch.setattr(ingestion, "load_assets", lambda: ["AAPL", "SPY"])

    def fake_download(symbol, start, end, progress):
        df = fake_yfinance_df.copy()
        return df

    monkeypatch.setattr(ingestion.yf, "download", fake_download)

    return bronze_dir


def test_ingestion_creates_files(isolated_ingestion_env):
    files = ingestion.ingest_all_assets(
        start_date="2024-01-01",
        end_date="2024-01-05",
    )

    assert len(files) == 2

    for file in files:
        path = Path(file)
        assert path.exists()
        assert path.stat().st_size > 0


def test_ingestion_csv_schema(isolated_ingestion_env):
    files = ingestion.ingest_all_assets(
        start_date="2024-01-01",
        end_date="2024-01-05",
    )

    expected_columns = {
        "Date",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "symbol",
    }

    for file in files:
        df = pd.read_csv(file)
        assert expected_columns.issubset(df.columns)


def test_ingestion_symbol_column(isolated_ingestion_env):
    files = ingestion.ingest_all_assets(
        start_date="2024-01-01",
        end_date="2024-01-05",
    )

    for file in files:
        df = pd.read_csv(file)
        assert df["symbol"].nunique() == 1
