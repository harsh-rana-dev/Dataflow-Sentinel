import pandas as pd
from datetime import date

from src.storage import get_engine, market_data, insert_silver_dataframe


def test_insert_is_idempotent():
    """
    Inserting the same silver data twice should not create duplicates.
    """

    # --- Arrange ---
    engine = get_engine()

    test_df = pd.DataFrame(
        [
            {
                "symbol": "TEST",
                "date": date(2024, 1, 1),
                "open": 100.0,
                "high": 110.0,
                "low": 90.0,
                "close": 105.0,
                "volume": 10000000,
            }
        ]
    )

    # Clean up any previous test data
    with engine.begin() as conn:
        conn.execute(
            market_data.delete().where(market_data.c.symbol == "TEST")
        )

    # --- Act ---
    insert_silver_dataframe(test_df)
    insert_silver_dataframe(test_df)  # insert same data again

    # --- Assert ---
    with engine.connect() as conn:
        result = conn.execute(
            market_data.select().where(market_data.c.symbol == "TEST")
        )
        rows = result.fetchall()

    assert len(rows) == 1



