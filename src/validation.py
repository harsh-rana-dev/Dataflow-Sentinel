from pathlib import Path
from datetime import date, datetime, timezone
from typing import List

import pandas as pd
from pydantic import BaseModel, ValidationError, Field

from src.logger import logger


SILVER_DIR = Path("data/silver")


class MarketDataRow(BaseModel):
    symbol: str = Field(min_length=1)
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int


def validate_bronze_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate raw bronze dataframe and return a clean silver dataframe.
    Invalid rows are dropped with metrics logged.
    """
    valid_rows: List[dict] = []
    rejected = 0

    for _, row in df.iterrows():
        try:
            record = MarketDataRow(
                symbol=row["symbol"],
                date=row["Date"],
                open=row["Open"],
                high=row["High"],
                low=row["Low"],
                close=row["Close"],
                volume=row["Volume"],
            )
            valid_rows.append(record.model_dump())

        except (ValidationError, KeyError, TypeError):
            rejected += 1

    silver_df = pd.DataFrame(valid_rows)

    logger.info(
        "Silver validation completed | valid_rows=%s rejected_rows=%s",
        len(silver_df),
        rejected,
    )

    return silver_df


def validate_bronze_csv(path: Path) -> pd.DataFrame:
    """
    Load a bronze CSV file and validate its contents.
    """
    logger.info("Validating bronze file: %s", path.name)

    df = pd.read_csv(path)
    return validate_bronze_dataframe(df)


def save_silver_dataframe(df: pd.DataFrame, source_file: Path) -> Path:
    """
    Persist validated silver dataframe to disk.
    """
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    run_date = datetime.now(timezone.utc).date().isoformat()
    output_name = f"{source_file.stem}_silver_{run_date}.csv"
    output_path = SILVER_DIR / output_name

    df.to_csv(output_path, index=False)

    logger.info("Silver data written: %s", output_path.as_posix())

    return output_path
