import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()


def get_engine():
    """Create and return a SQLAlchemy engine."""
    engine = create_engine(
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}",
        echo=False,
    )
    return engine

    

#Simple smoke test to verify database connection

def test_connection() -> None:
    
    engine = get_engine()

    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

    print("[DB] Connection successful")

