import os
from datetime import datetime, timezone
from typing import List
import yfinance as yf
import pandas as pd
import yaml

BRONZE_DIR = "data/bronze"

def load_assets(config_path: str = "config/assets.yaml") -> List[str]:
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path, "r") as file:
        config = yaml.safe_load(file) or {}
    return config.get("assets", [])

def fetch_asset_data(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    # Pro fix: API Key check (even if yfinance doesn't use it, your next provider will)
    api_key = os.getenv("ALPHAVANTAGE_API_KEY")
    
    try:
        df = yf.download(symbol, start=start_date, end=end_date, progress=False)
        # Fix: Flatten columns if yfinance returns multi-index
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
    except Exception as exc:
        print(f"[INGESTION] Failed to fetch {symbol}: {exc}")
        return pd.DataFrame()

    if df.empty:
        return df

    df.reset_index(inplace=True)
    df["symbol"] = symbol
    return df

def save_bronze_data(symbol: str, df: pd.DataFrame) -> str:
    os.makedirs(BRONZE_DIR, exist_ok=True)
    run_date = datetime.now(timezone.utc).date()
    file_path = f"{BRONZE_DIR}/{symbol}_{run_date}.csv"
    df.to_csv(file_path, index=False)
    return file_path

def ingest_all_assets(start_date: str, end_date: str) -> List[str]:
    assets = load_assets()
    saved_files = []
    print(f"[PIPELINE] Starting ingestion for {len(assets)} assets")
    for symbol in assets:
        df = fetch_asset_data(symbol, start_date, end_date)
        if not df.empty:
            path = save_bronze_data(symbol, df)
            saved_files.append(path)
    print(f"[PIPELINE] Ingestion complete. Files written: {len(saved_files)}")
    return saved_files