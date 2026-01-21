from datetime import datetime, timezone
from pathlib import Path
import json

import pandas as pd

from src.logger import get_logger


logger = get_logger(__name__)

SILVER_DIR = Path("data/silver")
GOLD_DIR = Path("data/gold")

AGGREGATES_FILE = GOLD_DIR / "aggregates.csv"
FRESHNESS_FILE = GOLD_DIR / "data_freshness.json"


def load_all_silver_data() -> pd.DataFrame:
    files = list(SILVER_DIR.glob("*.csv"))

    if not files:
        logger.error("No silver CSV files found in %s", SILVER_DIR)
        raise FileNotFoundError("No silver files found")

    logger.info("Loading %d silver files", len(files))

    dfs = [pd.read_csv(file, parse_dates=["date"]) for file in files]
    df = pd.concat(dfs, ignore_index=True)

    logger.debug("Loaded silver dataframe with %d rows", len(df))
    return df


def compute_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Computing gold aggregates")

    df = df.sort_values(["symbol", "date"])
    results = []

    for symbol, group in df.groupby("symbol"):
        latest = group.iloc[-1]

        results.append(
            {
                "symbol": symbol,
                "latest_date": latest["date"].date().isoformat(),
                "latest_close": latest["close"],
                "avg_7d_close": group["close"].tail(7).mean(),
                "avg_30d_close": group["close"].tail(30).mean(),
                "latest_volume": int(latest["volume"]),
            }
        )

    result_df = pd.DataFrame(results)
    logger.info("Computed aggregates for %d symbols", len(result_df))

    return result_df


def compute_data_freshness(df: pd.DataFrame) -> dict:
    logger.info("Computing data freshness signals")

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

    logger.info("Computed freshness for %d symbols", len(freshness))
    return freshness


def write_aggregates(df: pd.DataFrame) -> None:
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(AGGREGATES_FILE, index=False)

    logger.info("Aggregates written to %s", AGGREGATES_FILE)


def write_freshness(data: dict) -> None:
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    with open(FRESHNESS_FILE, "w") as f:
        json.dump(data, f, indent=2)

    logger.info("Freshness data written to %s", FRESHNESS_FILE)


def run_gold_layer() -> None:
    logger.info("Starting gold layer")

    silver_df = load_all_silver_data()

    aggregates = compute_aggregates(silver_df)
    write_aggregates(aggregates)

    freshness = compute_data_freshness(silver_df)
    write_freshness(freshness)

    logger.info("Gold layer completed successfully")
