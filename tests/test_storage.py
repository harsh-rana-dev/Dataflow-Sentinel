import os
import sys
from pathlib import Path

# 1. Force TESTING mode immediately to prevent accidental Neon connections
os.environ["TESTING"] = "1"

# 2. Setup paths so 'src' can be found locally and on GitHub
root = Path(__file__).parent.parent
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

import pandas as pd
from datetime import date
import pytest
from sqlalchemy import select, delete

# 3. Absolute imports
from src.storage import get_db_engine, market_data, insert_silver_dataframe, init_db

TEST_SYMBOL = "TEST"

@pytest.fixture(scope="function")
def setup_db():
    """Initializes the in-memory SQLite database for testing."""
    engine = init_db() 
    return engine

@pytest.fixture
def clean_test_rows(setup_db):
    engine = setup_db
    with engine.begin() as conn:
        conn.execute(delete(market_data))
    yield
    with engine.begin() as conn:
        conn.execute(delete(market_data))

def test_insert_is_idempotent(clean_test_rows, setup_db):
    engine = setup_db
    test_df = pd.DataFrame([{
        "symbol": TEST_SYMBOL,
        "date": date(2024, 1, 1),
        "open": 100.0, "high": 110.0, "low": 90.0, "close": 105.0,
        "volume": 10_000_000
    }])

    # Test Upsert logic (inserting twice shouldn't fail)
    insert_silver_dataframe(test_df)
    insert_silver_dataframe(test_df)

    with engine.connect() as conn:
        rows = conn.execute(select(market_data)).fetchall()

    assert len(rows) == 1
    assert rows[0].symbol == TEST_SYMBOL