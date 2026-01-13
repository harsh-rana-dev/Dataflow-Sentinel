import pandas as pd
from datetime import date
import pytest
from sqlalchemy import select, delete
from src.storage import get_engine, market_data, insert_silver_dataframe

TEST_SYMBOL = "TEST"

# ----------------------------
# 1️⃣ Fixture: clean up test rows before and after
# ----------------------------
@pytest.fixture
def clean_test_rows():
    engine = get_engine()
    # Delete any existing test rows
    with engine.begin() as conn:
        conn.execute(delete(market_data).where(market_data.c.symbol == TEST_SYMBOL))
    yield
    # Clean up after test as well
    with engine.begin() as conn:
        conn.execute(delete(market_data).where(market_data.c.symbol == TEST_SYMBOL))

# ----------------------------
# 2️⃣ Test: Insert is idempotent
# ----------------------------
def test_insert_is_idempotent(clean_test_rows):
    engine = get_engine()

    # --- Arrange ---
    test_df = pd.DataFrame(
        [
            {
                "symbol": TEST_SYMBOL,
                "date": date(2024, 1, 1),
                "open": 100.0,
                "high": 110.0,
                "low": 90.0,
                "close": 105.0,
                "volume": 10000000,
            }
        ]
    )

    # --- Act ---
    insert_silver_dataframe(test_df)
    insert_silver_dataframe(test_df)  # insert same data again

    # --- Assert ---
    with engine.connect() as conn:
        result = conn.execute(
            select(market_data).where(market_data.c.symbol == TEST_SYMBOL)
        )
        rows = result.fetchall()

    # Only one row should exist
    assert len(rows) == 1
    row = rows[0]
    assert row.symbol == TEST_SYMBOL
    assert row.date == date(2024, 1, 1)
