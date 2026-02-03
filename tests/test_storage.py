import pytest
import pandas as pd
from datetime import date
from sqlalchemy import select, delete
import src.storage as storage
from src.storage import get_db_engine, market_data, insert_silver_dataframe

# # Forces the storage module into TESTING mode and provides a clean engine
@pytest.fixture(scope="function")
def db_engine(monkeypatch):
    # IMPROVEMENT: Use monkeypatch instead of os.environ for cleaner isolation
    monkeypatch.setenv("TESTING", "1")
    
    # Reset the singleton to ensure a fresh in-memory DB for the test session
    storage._engine = None 
    engine = get_db_engine()
    return engine

# # Automatically clears the market_data table before every individual test
@pytest.fixture(autouse=True)
def clean_table(db_engine):
    with db_engine.begin() as conn:
        conn.execute(delete(market_data))
    yield

# # Verifies that a standard DataFrame is correctly mapped and saved to the DB
def test_insert_single_row(db_engine):
    df = pd.DataFrame([{
        "symbol": "AAPL",
        "date": date(2024, 1, 1),
        "open": 150.0, "high": 155.0, "low": 149.0, "close": 152.0,
        "volume": 1000000,
    }])

    insert_silver_dataframe(df)

    with db_engine.connect() as conn:
        result = conn.execute(select(market_data)).fetchall()
    
    assert len(result) == 1
    assert result[0].symbol == "AAPL"

# # Ensures that the UPSERT logic prevents duplicate entries for the same symbol and date
def test_insert_is_idempotent(db_engine):
    df = pd.DataFrame([{
        "symbol": "BTC",
        "date": date(2024, 1, 1),
        "open": 40000.0, "high": 41000.0, "low": 39000.0, "close": 40500.0,
        "volume": 500,
    }])

    # Insert twice
    insert_silver_dataframe(df)
    insert_silver_dataframe(df)

    with db_engine.connect() as conn:
        rows = conn.execute(select(market_data)).fetchall()

    # Should still only have 1 row due to UniqueConstraint
    assert len(rows) == 1

# # Confirms the guardrail logic prevents errors when the pipeline passes no data
def test_empty_dataframe_is_noop(db_engine):
    # This triggers the "No data provided" warning in your logs
    insert_silver_dataframe(pd.DataFrame())

    with db_engine.connect() as conn:
        rows = conn.execute(select(market_data)).fetchall()

    assert len(rows) == 0

# # IMPROVEMENT: Tests the batching logic by inserting more rows than the default batch size
def test_insert_large_batch(db_engine):
    # Create 600 rows (default batch is 500)
    records = []
    for i in range(600):
        records.append({
            "symbol": "TSLA",
            "date": date(2020, 1, 1) + pd.Timedelta(days=i),
            "open": 100.0, "high": 110.0, "low": 90.0, "close": 105.0,
            "volume": 1000,
        })
    df = pd.DataFrame(records)

    insert_silver_dataframe(df, batch_size=100)

    with db_engine.connect() as conn:
        count = conn.execute(select(market_data)).fetchall()
    
    assert len(count) == 600