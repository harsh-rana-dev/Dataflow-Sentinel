import os
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    BigInteger,
    Integer,
    String,
    Date,
    Float,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from src.logger import get_logger

logger = get_logger(__name__)
load_dotenv()

metadata = MetaData()
_engine: Optional[object] = None

# --- Database Engine ---

def get_engine():

    if os.getenv("TESTING") == "1":
        logger.info("Using in-memory SQLite database for testing")
        return create_engine("sqlite:///:memory:", echo=False)

    conn_str = os.getenv("DATABASE_URL")

    if not conn_str:
        logger.error("Missing DATABASE_URL in environment variables")
        raise RuntimeError("Database configuration incomplete")

    if conn_str.startswith("postgres://"):
        conn_str = conn_str.replace("postgres://", "postgresql://", 1)

    logger.info("Creating PostgreSQL engine from DATABASE_URL")
    return create_engine(
        conn_str, 
        echo=False, 
        pool_pre_ping=True 
    )

def get_db_engine():
    """Singleton DB engine initializer."""
    global _engine

    if _engine is None:
        _engine = get_engine()
        metadata.create_all(_engine)
        logger.info("Database tables ensured")

    return _engine

# --- Table Definition ---

market_data = Table(
    "market_data",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("symbol", String(10), nullable=False),
    Column("date", Date, nullable=False),
    Column("open", Float, nullable=False),
    Column("high", Float, nullable=False),
    Column("low", Float, nullable=False),
    Column("close", Float, nullable=False),
    Column("volume", BigInteger, nullable=False),
    UniqueConstraint("symbol", "date", name="uq_symbol_date"),
)

# --- Insert Logic (Idempotent) ---

def insert_silver_dataframe(df: pd.DataFrame, batch_size: int = 500) -> None:
    if df is None or df.empty:
        logger.warning("No data provided to insert into silver layer")
        return

    engine = get_db_engine()
    records = df.to_dict(orient="records")

    is_sqlite = engine.dialect.name == "sqlite"
    insert_fn = sqlite_insert if is_sqlite else pg_insert

    logger.info("Inserting %d records into market_data", len(records))

    with engine.begin() as conn:
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            stmt = insert_fn(market_data).values(batch)

            if is_sqlite:
                stmt = stmt.prefix_with("OR IGNORE")
            else:
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=["symbol", "date"]
                )

            conn.execute(stmt)

    logger.info("Silver-layer insert completed")