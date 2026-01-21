import os
import sys
from pathlib import Path
from datetime import date

import pandas as pd
import pytest
from sqlalchemy import select, delete

# --------------------------------------------------
# Force TESTING mode BEFORE importing storage
# --------------------------------------------------
os.environ["TESTING"] = "1"

# --------------------------------------------------
# Ensure src is importable (local + CI)
# --------------------------------------------------
root = Path(__file__).parent.parent
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

# --------------------------------------------------
# Imports
# --------------------------------------------------
from src.storage import get_db_engine, market_data, insert_silver_dataframe


TEST_SYMBOL = "TEST"


@pytest.fixture(scope="function")
def db_engine():
    """Provide a fresh in-memory SQLite engine."""
    engine = get_db_engine()
    return engine


@pytest.fixture(autouse=True)
def clean_table(db_engine):
    """Ensure market_data table is clean before and after each test."""
    with db_engine.begin() as conn:
        conn.execute(delete(market_data))
    yield
    with db_engine.begin() as conn:
        conn.execute(delete(market_data))


def test_insert_single_row(db_engine):
    df = pd.DataFrame(
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

    insert_silver_dataframe(df)

    with db_engine.connect() as conn:
        rows = conn.execute(select(market_data)).fetchall()

    assert len(rows) == 1
    assert rows[0].symbol == TEST_SYMBOL


def test_insert_is_idempotent(db_engine):
    """Inserting the same row twice should not duplicate data."""
    df = pd.DataFrame(
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

    insert_silver_dataframe(df)
    insert_silver_dataframe(df)

    with db_engine.connect() as conn:
        rows = conn.execute(select(market_data)).fetchall()

    assert len(rows) == 1


def test_empty_dataframe_is_noop(db_engine):
    """Empty dataframe should not insert anything."""
    insert_silver_dataframe(pd.DataFrame())

    with db_engine.connect() as conn:
        rows = conn.execute(select(market_data)).fetchall()

    assert rows == []
