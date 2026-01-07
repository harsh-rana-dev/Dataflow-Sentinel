import os
from datetime import datetime, timezone
from typing import List

import yfinance as yf
import pandas as pd
import yaml

BRONZE_DIR = "data/bronze"


def load_assets(config_path: str = "config/assets.yaml") -> List[str]:
    """
    Load asset symbols from YAML configuration.
    
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as file:
        config = yaml.safe_load(file) or {}

    assets = config.get("assets")
    if not isinstance(assets, list):
        raise ValueError("Invalid assets configuration: expected a list")

    return assets


def fetch_asset_data(
    symbol: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Fetch historical market data for a single asset."""
    try:
        df = yf.download(
            symbol,
            start=start_date,
            end=end_date,
            progress=False,
        )
    except Exception as exc:
        print(f"[INGESTION] Failed to fetch {symbol}: {exc}")
        return pd.DataFrame()

    if df.empty:
        print(f"[INGESTION] No data returned for {symbol}")
        return df

    df.reset_index(inplace=True)
    df["symbol"] = symbol
    return df


def save_bronze_data(symbol: str, df: pd.DataFrame) -> str:
    """Save raw market data to the bronze layer."""
    os.makedirs(BRONZE_DIR, exist_ok=True)

    run_date = datetime.now(timezone.utc).date()
    file_path = f"{BRONZE_DIR}/{symbol}_{run_date}.csv"

    df.to_csv(file_path, index=False)
    return file_path


def ingest_all_assets(
    start_date: str,
    end_date: str,
) -> List[str]:
    
    assets = load_assets()
    saved_files: List[str] = []

    print(f"[PIPELINE] Starting ingestion for {len(assets)} assets")

    for symbol in assets:
        print(f"[PIPELINE] Ingesting {symbol}")

        df = fetch_asset_data(symbol, start_date, end_date)
        if df.empty:
            continue

        path = save_bronze_data(symbol, df)
        saved_files.append(path)

        print(f"[PIPELINE] Saved bronze data: {path}")

    print(f"[PIPELINE] Ingestion complete. Files written: {len(saved_files)}")
    return saved_files
