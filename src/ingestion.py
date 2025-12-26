import os
from datetime import datetime
import yfinance as yf
import pandas as pd
import yaml


BRONZE_DIR = "data/bronze"


def load_symbols(path: str = "config/assets.yaml") -> list[str]:
    with open(path, "r") as f:
        return yaml.safe_load(f).get("assets", [])


def fetch_data(symbol: str) -> pd.DataFrame:
    df = yf.Ticker(symbol).history(period="7d")

    if df.empty:
        raise ValueError(f"No data for symbol: {symbol}")

    df = df.reset_index()
    df["symbol"] = symbol
    return df


def save_bronze(symbol: str, df: pd.DataFrame) -> str:
    os.makedirs(BRONZE_DIR, exist_ok=True)
    filename = f"{BRONZE_DIR}/{symbol}_{datetime.utcnow().date()}.csv"
    df.to_csv(filename, index=False)
    return filename


def ingest() -> list[str]:
    files = []

    for symbol in load_symbols():
        df = fetch_data(symbol)
        path = save_bronze(symbol, df)
        files.append(path)

    return files

