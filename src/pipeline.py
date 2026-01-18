import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Fix: Ensure the project root is in the system path. 
# This prevents "ModuleNotFoundError" when running in GitHub Actions.
root = Path(__file__).parent.parent
if str(root) not in sys.path:
    sys.path.append(str(root))

from src.ingestion import ingest_all_assets
from src.validation import validate_bronze_csv, save_silver_dataframe
from src.storage import insert_silver_dataframe, init_db
from src.gold import run_gold_layer
from src.logger import logger 

BRONZE_DIR = Path("data/bronze")

def run_pipeline() -> None:
    logger.info("[PIPELINE] Starting pipeline")
    
    # 0️⃣ Database Setup (FORCED INITIALIZATION)
    # This ensures tables exist in Neon before we try to insert data.
    try:
        init_db()
    except Exception:
        logger.error("[PIPELINE] Critical: Database setup failed. Exiting.")
        return

    end_date = datetime.now(timezone.utc).date().isoformat()

    # 1️⃣ Ingestion: Fetch data from the API
    try:
        ingest_all_assets(
            start_date="2020-01-01",
            end_date=end_date,
        )
        logger.info("[PIPELINE] Ingestion complete")
    except Exception as e:
        logger.error(f"[PIPELINE] Ingestion failed: {e}")
        return

    # 2️⃣ Validation + Silver persistence (Local CSV + Neon Upload)
    bronze_files = list(BRONZE_DIR.glob("*.csv"))

    if not bronze_files:
        logger.warning("[PIPELINE] No bronze files found in data/bronze. Check ingestion logs.")
    else:
        for bronze_file in bronze_files:
            logger.info(f"[PIPELINE] Processing {bronze_file.name}")

            try:
                # Clean and validate the data
                silver_df = validate_bronze_csv(bronze_file)
                
                if silver_df.empty:
                    logger.warning(f"[PIPELINE] No valid rows in {bronze_file.name}, skipping")
                    continue

                # Save locally for backup (Silver Layer)
                save_silver_dataframe(silver_df, bronze_file)
                
                # 3️⃣ Store in Neon (Postgres)
                insert_silver_dataframe(silver_df)
                logger.info(f"[PIPELINE] Successfully stored data from {bronze_file.name}")
                
            except Exception as e:
                logger.error(f"[PIPELINE] Failed to process {bronze_file.name}: {e}")

    # 4️⃣ Gold layer: Aggregate data for reporting
    try:
        logger.info("[PIPELINE] Running Gold layer aggregates")
        run_gold_layer()
        logger.info("[PIPELINE] Gold layer finished successfully")
    except Exception as e:
        logger.error(f"[PIPELINE] Gold layer failed: {e}")

    logger.info("[PIPELINE] Pipeline finished successfully")

if __name__ == "__main__":
    run_pipeline()