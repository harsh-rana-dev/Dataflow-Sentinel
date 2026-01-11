from datetime import datetime, timezone
from pathlib import Path

from ingestion import ingest_all_assets
from validation import (
    validate_bronze_csv,
    save_silver_dataframe,
)
from storage import insert_silver_dataframe


BRONZE_DIR = Path("data/bronze")


def run_pipeline() -> None:
    

    print("[PIPELINE] Starting pipeline")

    end_date = datetime.now(timezone.utc).date().isoformat()

    # 1️⃣ Ingestion
    ingest_all_assets(
        start_date="2020-01-01",
        end_date=end_date,
    )

    print("[PIPELINE] Ingestion complete")


    # 2️⃣ Validation + Silver persistence
    bronze_files = list(BRONZE_DIR.glob("*.csv"))

    if not bronze_files:
        print("[PIPELINE] No bronze files found")
        return

    for bronze_file in bronze_files:
        print(f"[PIPELINE] Processing {bronze_file.name}")

        silver_df = validate_bronze_csv(bronze_file)

        if silver_df.empty:
            print("[PIPELINE] No valid rows, skipping")
            continue

        silver_path = save_silver_dataframe(silver_df, bronze_file)
        print(f"[PIPELINE] Silver saved → {silver_path.name}")

        # 3️⃣ Store in Postgres
        insert_silver_dataframe(silver_df)
        print("[PIPELINE] Data inserted into database")


    print("[PIPELINE] Pipeline finished successfully")


if __name__ == "__main__":
    run_pipeline()
