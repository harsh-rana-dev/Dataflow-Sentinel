from datetime import datetime, timezone
from pathlib import Path
import json

import pandas as pd


SILVER_DIR = Path("data/silver")
GOLD_DIR = Path("data/gold")

AGGREGATES_FILE = GOLD_DIR / "aggregates.csv"
FRESHNESS_FILE = GOLD_DIR / "data_freshness.json"


def load_all_silver_data() -> pd.DataFrame:
    files = list(SILVER_DIR.glob("*.csv"))

    if not files:
        raise FileNotFoundError("No silver files found")

    dfs = [pd.read_csv(file, parse_dates=["date"]) for file in files]
    return pd.concat(dfs, ignore_index=True)


def compute_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["symbol", "date"])

    results = []

    for symbol, group in df.groupby("symbol"):
        latest = group.iloc[-1]

        result = {
            "symbol": symbol,
            "latest_date": latest["date"].date().isoformat(),
            "latest_close": latest["close"],
            "avg_7d_close": group["close"].tail(7).mean(),
            "avg_30d_close": group["close"].tail(30).mean(),
            "latest_volume": int(latest["volume"]),
        }

        results.append(result)

    return pd.DataFrame(results)


def compute_data_freshness(df: pd.DataFrame) -> dict:
    """Compute freshness signals per symbol."""
    today = datetime.now(timezone.utc).date()
    freshness = {}

    for symbol, group in df.groupby("symbol"):
        last_date = group["date"].max().date()
        days_since = (today - last_date).days

        freshness[symbol] = {
            "last_available_date": last_date.isoformat(),
            "days_since_update": days_since,
            "is_stale": days_since > 2,
        }

    return freshness


def write_aggregates(df: pd.DataFrame) -> None:
    """Write aggregates CSV to gold layer."""
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(AGGREGATES_FILE, index=False)


def write_freshness(data: dict) -> None:
    """Write data freshness JSON to gold layer."""
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    with open(FRESHNESS_FILE, "w") as f:
        json.dump(data, f, indent=2)


def run_gold_layer() -> None:
    """Execute full gold-layer computation."""
    print("[GOLD] Starting gold layer")

    silver_df = load_all_silver_data()

    aggregates = compute_aggregates(silver_df)
    write_aggregates(aggregates)
    print(f"[GOLD] Aggregates written → {AGGREGATES_FILE.name}")

    freshness = compute_data_freshness(silver_df)
    write_freshness(freshness)
    print(f"[GOLD] Freshness written → {FRESHNESS_FILE.name}")

    print("[GOLD] Gold layer completed successfully")
