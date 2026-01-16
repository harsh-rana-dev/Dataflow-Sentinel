import pandas as pd
from datetime import date
import pytest

from sqlalchemy import select, delete
from src.storage import get_db_engine, market_data, insert_silver_dataframe

TEST_SYMBOL = "TEST"


# ----------------------------
# 1️⃣ Fixture: clean test rows
# ----------------------------
@pytest.fixture
def clean_test_rows():
    engine = get_db_engine()

    with engine.begin() as conn:
        conn.execute(
            delete(market_data).where(market_data.c.symbol == TEST_SYMBOL)
        )

    yield

    with engine.begin() as conn:
        conn.execute(
            delete(market_data).where(market_data.c.symbol == TEST_SYMBOL)
        )


# ----------------------------
# 2️⃣ Test: idempotent insert
# ----------------------------
def test_insert_is_idempotent(clean_test_rows):
    engine = get_db_engine()

    test_df = pd.DataFrame(
        [
            {
                "symbol": TEST_SYMBOL,
                "date": date(2024, 1, 1),
                "open": 100.0,
                "high": 110.0,
                "low": 90.0,
                "close": 105.0,
                "volume": 10_000_000,
            }
        ]
    )

    insert_silver_dataframe(test_df)
    insert_silver_dataframe(test_df)  # same data again

    with engine.connect() as conn:
        result = conn.execute(
            select(market_data).where(market_data.c.symbol == TEST_SYMBOL)
        )
        rows = result.fetchall()

    assert len(rows) == 1
    assert rows[0].symbol == TEST_SYMBOL
    assert rows[0].date == date(2024, 1, 1)
