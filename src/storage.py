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
from sqlalchemy.dialects.postgresql import insert


load_dotenv()


# ----------------------------
# 1️⃣ Database Engine
# ----------------------------

def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}",
        echo=False,
    )


engine = get_engine()
metadata = MetaData()


# ----------------------------
# 2️⃣ Table Definition
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


metadata.create_all(engine)


# ----------------------------
# 3️⃣ Insert / Upsert Logic (Batched)
# ----------------------------

def insert_silver_dataframe(df: pd.DataFrame, batch_size: int = 500) -> None:


    records = df.to_dict(orient="records")
    if not records:
        return

    with engine.begin() as conn:
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            stmt = insert(market_data).values(batch)
            stmt = stmt.on_conflict_do_nothing(index_elements=["symbol", "date"])
            conn.execute(stmt)

            