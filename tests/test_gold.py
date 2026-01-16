from pathlib import Path
import json
import pandas as pd

import pytest

from src.gold import run_gold_layer


GOLD_DIR = Path("data/gold")
AGG_FILE = GOLD_DIR / "aggregates.csv"
FRESH_FILE = GOLD_DIR / "data_freshness.json"


@pytest.fixture
def mock_silver_data(monkeypatch):

    fake_df = pd.DataFrame(
        {
            "symbol": ["AAPL", "BTC-USD"],
            "date": pd.to_datetime(["2026-01-10", "2026-01-10"]),
            "close": [150.0, 42000.0],
            "volume": [1000000, 500000],
        }
    )

    monkeypatch.setattr(
        "src.gold.load_all_silver_data",
        lambda: fake_df,
    )


def test_gold_outputs_created(mock_silver_data):
    """
    Gold layer should create its output artifacts.
    """
    run_gold_layer()

    assert AGG_FILE.exists(), "aggregates.csv was not created"
    assert FRESH_FILE.exists(), "data_freshness.json was not created"


def test_gold_aggregate_content(mock_silver_data):
    """
    Gold aggregates should contain expected columns and valid data.
    """
    run_gold_layer()

    df = pd.read_csv(AGG_FILE)

    expected_columns = {
        "symbol",
        "latest_date",
        "latest_close",
        "avg_7d_close",
        "avg_30d_close",
        "latest_volume",
    }

    assert expected_columns.issubset(df.columns)

    assert df["symbol"].isnull().sum() == 0
    assert (df["avg_7d_close"] > 0).all()
    assert (df["avg_30d_close"] > 0).all()


def test_gold_freshness_logic(mock_silver_data):
    """
    Freshness JSON should contain sane values.
    """
    run_gold_layer()

    with open(FRESH_FILE) as f:
        data = json.load(f)

    assert isinstance(data, dict)
    assert len(data) > 0

    for symbol, info in data.items():
        assert "last_available_date" in info
        assert "days_since_update" in info
        assert "is_stale" in info
        assert info["days_since_update"] >= 0
