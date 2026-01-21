import os
from datetime import datetime, timezone
from typing import List

import pandas as pd
import yfinance as yf
import yaml

from src.logger import get_logger


logger = get_logger(__name__)

BRONZE_DIR = "data/bronze"
ASSETS_CONFIG_PATH = "config/assets.yaml"


def load_assets(config_path: str = ASSETS_CONFIG_PATH) -> List[str]:
    if not os.path.exists(config_path):
        logger.error("Assets config file not found: %s", config_path)
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as file:
        config = yaml.safe_load(file) or {}

    assets = config.get("assets", [])
    logger.info("Loaded %d assets from config", len(assets))

    return assets


def fetch_asset_data(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    logger.info("Fetching data for asset: %s", symbol)

    try:
        df = yf.download(
            symbol,
            start=start_date,
            end=end_date,
            progress=False,
        )

        # Handle multi-index columns (yfinance quirk)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

    except Exception as exc:
        logger.error("Failed to fetch data for %s: %s", symbol, exc)
        return pd.DataFrame()

    if df.empty:
        logger.warning("No data returned for asset: %s", symbol)
        return df

    df.reset_index(inplace=True)
    df["symbol"] = symbol

    logger.debug("Fetched %d rows for %s", len(df), symbol)
    return df


def save_bronze_data(symbol: str, df: pd.DataFrame) -> str:
    os.makedirs(BRONZE_DIR, exist_ok=True)

    run_date = datetime.now(timezone.utc).date()
    file_path = f"{BRONZE_DIR}/{symbol}_{run_date}.csv"

    df.to_csv(file_path, index=False)

    logger.info("Bronze data written: %s", file_path)
    return file_path


def ingest_all_assets(start_date: str, end_date: str) -> List[str]:
    assets = load_assets()
    saved_files: List[str] = []

    logger.info("Starting ingestion for %d assets", len(assets))

    for symbol in assets:
        df = fetch_asset_data(symbol, start_date, end_date)

        if df.empty:
            logger.warning("Skipping asset with no data: %s", symbol)
            continue

        path = save_bronze_data(symbol, df)
        saved_files.append(path)

    logger.info("Ingestion completed. %d files written", len(saved_files))
    return saved_files
