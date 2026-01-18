import os
import urllib.parse
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import (
    create_engine, MetaData, Table, Column, BigInteger, 
    Integer, String, Date, Float, UniqueConstraint, text, inspect
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

try:
    from src.logger import logger
except ImportError:
    from logger import logger

load_dotenv()

metadata = MetaData()
_engine = None

def get_engine():
    # If testing, strictly use SQLite
    if os.getenv("TESTING") == "1":
        return create_engine("sqlite:///:memory:", echo=False)

    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT', 5432)
    name = os.getenv('DB_NAME')
    
    if not all([user, password, host, name]):
        logger.error("[STORAGE] Missing DB credentials in environment variables!")

    # Fix: URL-encode the password to handle special characters (@, #, /) in Neon
    safe_password = urllib.parse.quote_plus(password) if password else ""

    # Fix: Add sslmode=require for Neon Cloud compatibility
    conn_str = f"postgresql+psycopg2://{user}:{safe_password}@{host}:{port}/{name}?sslmode=require"
    
    # Fix: pool_pre_ping checks the connection before using it (good for serverless DBs)
    return create_engine(conn_str, echo=False, pool_pre_ping=True)

def get_db_engine():
    global _engine
    if _engine is None:
        _engine = get_engine()
        metadata.create_all(_engine)
    return _engine

def init_db():
    engine = get_db_engine()
    try:
        with engine.begin() as conn:
            metadata.create_all(engine)
            conn.execute(text("SELECT 1"))
        
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        logger.info(f"🔍 [DB CHECK] Tables found: {existing_tables}")
    except Exception as e:
        logger.error(f"❌ [STORAGE] DB Init failed: {e}")
        raise e
    return engine

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
            if not is_sqlite:
                stmt = stmt.on_conflict_do_nothing(index_elements=["symbol", "date"])
            else:
                stmt = stmt.prefix_with("OR IGNORE")
            conn.execute(stmt)