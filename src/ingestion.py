import os
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone
from typing import List
from pathlib import Path
from src.logger import get_logger

logger = get_logger(__name__)

# # Downloads historical market data from Yahoo Finance for a specific symbol
def fetch_asset_data(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    logger.info(f"Fetching: {symbol}")
    try:
        df = yf.download(symbol, start=start_date, end=end_date, progress=False)
        
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
            
        if df.empty:
            return pd.DataFrame()

        df.reset_index(inplace=True)
        df["symbol"] = symbol
        return df
    except Exception as exc:
        logger.error(f"Error fetching {symbol}: {exc}")
        return pd.DataFrame()

# # Saves the downloaded DataFrame as a CSV file in the Bronze directory
def save_bronze_data(symbol: str, df: pd.DataFrame, bronze_dir: Path) -> str:
    os.makedirs(bronze_dir, exist_ok=True)
    run_date = datetime.now(timezone.utc).date()
    file_path = bronze_dir / f"{symbol}_{run_date}.csv"

    df.to_csv(file_path, index=False)
    logger.info(f"Saved Bronze file: {file_path}")
    return str(file_path)

# # Orchestrates the fetching and saving process for the entire list of assets
def ingest_all_assets(tickers: List[str], start_date: str, end_date: str, bronze_dir: Path) -> List[str]:
    saved_files: List[str] = []
    
    for symbol in tickers:
        df = fetch_asset_data(symbol, start_date, end_date)

        if not df.empty:
            path = save_bronze_data(symbol, df, bronze_dir)
            saved_files.append(path)
        else:
            logger.warning(f"No data to save for {symbol}")

    return saved_files
    