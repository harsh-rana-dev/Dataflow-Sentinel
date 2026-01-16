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
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

load_dotenv()

# ----------------------------
# 1️⃣ Engine factory
# ----------------------------

metadata = MetaData()
_engine = None


def get_engine():
    """
    - SQLite in-memory for tests
    - Postgres for prod/dev
    """
    if os.getenv("TESTING") == "1":
        return create_engine("sqlite:///:memory:", echo=False)

    return create_engine(
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', 5432)}/{os.getenv('DB_NAME')}",
        echo=False,
    )


def get_db_engine():
    global _engine
    if _engine is None:
        _engine = get_engine()
        metadata.create_all(_engine)
    return _engine


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
# 3️⃣ Insert logic (DB-aware)
# ----------------------------

def insert_silver_dataframe(df: pd.DataFrame, batch_size: int = 500) -> None:
    records = df.to_dict(orient="records")
    if not records:
        return

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
