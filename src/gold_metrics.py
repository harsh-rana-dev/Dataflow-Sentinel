from datetime import datetime, timezone
from pathlib import Path
import json
import pandas as pd
from src.logger import get_logger

logger = get_logger(__name__)

# # Loads all available silver files to create a unified dataset for analysis
def load_all_silver_data(silver_dir: Path) -> pd.DataFrame:
    files = list(silver_dir.glob("*.csv"))
    if not files:
        logger.error(f"No silver files found in {silver_dir}")
        raise FileNotFoundError("Empty Silver Layer")

    dfs = [pd.read_csv(file, parse_dates=["date"]) for file in files]
    return pd.concat(dfs, ignore_index=True)

# # Calculates moving averages and latest price points for each asset
def compute_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Computing gold aggregates")
    df = df.sort_values(["symbol", "date"])
    results = []

    for symbol, group in df.groupby("symbol"):
        latest = group.iloc[-1]
        results.append({
            "symbol": symbol,
            "latest_date": latest["date"].date().isoformat(),
            "latest_close": latest["close"],
            "avg_7d_close": group["close"].tail(7).mean(),
            "avg_30d_close": group["close"].tail(30).mean(),
            "latest_volume": int(latest["volume"]),
        })
    return pd.DataFrame(results)

# # Checks the time difference between the last data point and today
def compute_data_freshness(df: pd.DataFrame) -> dict:
    today = datetime.now(timezone.utc).date()
    freshness = {}

    for symbol, group in df.groupby("symbol"):
        last_date = group["date"].max().date()
        days_since = (today - last_date).days
        freshness[symbol] = {
            "last_date": last_date.isoformat(),
            "days_stale": days_since,
            "status": "STALE" if days_since > 2 else "FRESH"
        }
    return freshness

# # Orchestrates the gold layer transformation using paths from config
def run_gold_layer(silver_dir: Path, gold_dir: Path) -> None:
    logger.info("Starting Gold Layer processing")
    gold_dir.mkdir(parents=True, exist_ok=True)
    
    silver_df = load_all_silver_data(silver_dir)
    
    # Save Aggregates
    aggregates = compute_aggregates(silver_df)
    aggregates.to_csv(gold_dir / "aggregates.csv", index=False)
    
    # Save Freshness
    freshness = compute_data_freshness(silver_df)
    with open(gold_dir / "freshness.json", "w") as f:
        json.dump(freshness, f, indent=2)

    logger.info("Gold metrics written successfully")