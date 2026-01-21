from pathlib import Path
import json
import pandas as pd
import pytest

import src.gold_metrics as gold


@pytest.fixture
def fake_silver_df():
    return pd.DataFrame(
        {
            "symbol": ["AAPL", "AAPL", "BTC-USD"],
            "date": pd.to_datetime(
                ["2026-01-08", "2026-01-10", "2026-01-10"]
            ),
            "close": [145.0, 150.0, 42000.0],
            "volume": [900000, 1000000, 500000],
        }
    )


@pytest.fixture
def isolated_gold_env(tmp_path, monkeypatch, fake_silver_df):
    """
    Fully isolate gold layer from real filesystem and IO.
    """
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    silver_dir.mkdir()
    gold_dir.mkdir()

    monkeypatch.setattr(gold, "SILVER_DIR", silver_dir)
    monkeypatch.setattr(gold, "GOLD_DIR", gold_dir)
    monkeypatch.setattr(gold, "AGGREGATES_FILE", gold_dir / "aggregates.csv")
    monkeypatch.setattr(gold, "FRESHNESS_FILE", gold_dir / "data_freshness.json")

    monkeypatch.setattr(gold, "load_all_silver_data", lambda: fake_silver_df)

    return gold_dir


def test_gold_creates_output_files(isolated_gold_env):
    gold.run_gold_layer()

    assert (isolated_gold_env / "aggregates.csv").exists()
    assert (isolated_gold_env / "data_freshness.json").exists()


def test_gold_aggregates_schema_and_values(isolated_gold_env):
    gold.run_gold_layer()

    df = pd.read_csv(isolated_gold_env / "aggregates.csv")

    expected_columns = {
        "symbol",
        "latest_date",
        "latest_close",
        "avg_7d_close",
        "avg_30d_close",
        "latest_volume",
    }

    assert expected_columns.issubset(df.columns)
    assert df["symbol"].nunique() == 2
    assert (df["latest_close"] > 0).all()
    assert (df["latest_volume"] > 0).all()


def test_gold_freshness_contract(isolated_gold_env):
    gold.run_gold_layer()

    with open(isolated_gold_env / "data_freshness.json") as f:
        data = json.load(f)

    assert isinstance(data, dict)
    assert set(data.keys()) == {"AAPL", "BTC-USD"}

    for payload in data.values():
        assert "last_available_date" in payload
        assert "days_since_update" in payload
        assert "is_stale" in payload
        assert payload["days_since_update"] >= 0
