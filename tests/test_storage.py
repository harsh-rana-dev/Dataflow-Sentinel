import os
# Force TESTING mode before any other imports so storage.py picks up SQLite
os.environ["TESTING"] = "1"

import pandas as pd
from datetime import date
import pytest

from sqlalchemy import select, delete
# Ensure we import from src.storage
from src.storage import get_db_engine, market_data, insert_silver_dataframe

TEST_SYMBOL = "TEST"

@pytest.fixture(scope="function")
def engine():
    """Provides a fresh engine for every test."""
    # Calling get_db_engine() triggers metadata.create_all() for the SQLite engine
    return get_db_engine()

@pytest.fixture
def clean_test_rows(engine):
    """Cleans the table before and after each test."""
    with engine.begin() as conn:
        conn.execute(delete(market_data))
    yield
    with engine.begin() as conn:
        conn.execute(delete(market_data))

def test_insert_is_idempotent(engine, clean_test_rows):
    """Verifies that inserting the same data twice doesn't create duplicates."""
    
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

    # First Insert
    insert_silver_dataframe(test_df)
    # Second Insert (Idempotency check)
    insert_silver_dataframe(test_df)

    with engine.connect() as conn:
        result = conn.execute(select(market_data))
        rows = result.fetchall()

    # Assertions
    assert len(rows) == 1, f"Expected 1 row, found {len(rows)}"
    assert rows[0].symbol == TEST_SYMBOL