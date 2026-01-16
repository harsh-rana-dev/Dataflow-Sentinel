import os
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
    text
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

# Use src.logger to avoid ModuleNotFoundError in GitHub Actions
try:
    from src.logger import logger
except ImportError:
    from logger import logger

load_dotenv()

# ----------------------------
# 1️⃣ Engine factory
# ----------------------------

metadata = MetaData()
_engine = None

def get_engine():
    if os.getenv("TESTING") == "1":
        return create_engine("sqlite:///:memory:", echo=False)

    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT', 5432)
    name = os.getenv('DB_NAME')
    
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
    return create_engine(conn_str, echo=False)

def get_db_engine():
    global _engine
    if _engine is None:
        _engine = get_engine()
        metadata.create_all(_engine)
    return _engine

def init_db():
    """Forces engine creation and table schema deployment immediately."""
    engine = get_db_engine()
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info(f"[STORAGE] Database initialized. Host: {engine.url.host}")
    except Exception as e:
        logger.error(f"[STORAGE] Database connection failed: {e}")
        raise e
    return engine

# ----------------------------
# 2️⃣ Table definition
# ----------------------------

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

# ----------------------------
# 3️⃣ Insert logic
# ----------------------------

def insert_silver_dataframe(df: pd.DataFrame, batch_size: int = 500) -> None:
    if df is None or df.empty:
        return

    records = df.to_dict(orient="records")
    engine = get_db_engine()

    is_sqlite = engine.dialect.name == "sqlite"
    insert_fn = sqlite_insert if is_sqlite else pg_insert

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