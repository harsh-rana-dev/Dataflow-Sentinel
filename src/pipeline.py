from datetime import datetime, timezone
from pathlib import Path

from ingestion import ingest_all_assets
from validation import (
    validate_bronze_csv,
    save_silver_dataframe,
)
from storage import insert_silver_dataframe
from gold import run_gold_layer

from logger import logger 

BRONZE_DIR = Path("data/bronze")


def run_pipeline() -> None:
    logger.info("[PIPELINE] Starting pipeline")

    end_date = datetime.now(timezone.utc).date().isoformat()

    # 1️⃣ Ingestion
    try:
        ingest_all_assets(
            start_date="2020-01-01",
            end_date=end_date,
        )
        logger.info("[PIPELINE] Ingestion complete")
    except Exception as e:
        logger.error(f"[PIPELINE] Ingestion failed: {e}")
        return


    # 2️⃣ Validation + Silver persistence
    bronze_files = list(BRONZE_DIR.glob("*.csv"))

    if not bronze_files:
        logger.warning("[PIPELINE] No bronze files found")
        return

    for bronze_file in bronze_files:
        logger.info(f"[PIPELINE] Processing {bronze_file.name}")

        try:
            silver_df = validate_bronze_csv(bronze_file)
        except Exception as e:
            logger.error(f"[PIPELINE] Validation failed for {bronze_file.name}: {e}")
            continue

        if silver_df.empty:
            logger.warning(f"[PIPELINE] No valid rows in {bronze_file.name}, skipping")
            continue

        try:
            silver_path = save_silver_dataframe(silver_df, bronze_file)
            logger.info(f"[PIPELINE] Silver saved → {silver_path.name}")
        except Exception as e:
            logger.error(f"[PIPELINE] Failed to save silver for {bronze_file.name}: {e}")
            continue


        # 3️⃣ Store in Postgres
        try:
            insert_silver_dataframe(silver_df)
            logger.info(f"[PIPELINE] Data inserted into database for {bronze_file.name}")
        except Exception as e:
            logger.error(f"[PIPELINE] Database insert failed for {bronze_file.name}: {e}")


    # 4️⃣ Gold layer
    try:
        logger.info("[PIPELINE] Running Gold layer")
        run_gold_layer()
        logger.info("[PIPELINE] Gold layer finished successfully")
    except Exception as e:
        logger.error(f"[PIPELINE] Gold layer failed: {e}")

    logger.info("[PIPELINE] Pipeline finished successfully")


if __name__ == "__main__":
    run_pipeline()
