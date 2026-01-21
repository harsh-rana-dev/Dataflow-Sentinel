from datetime import datetime, timezone
from pathlib import Path

from src.ingestion import ingest_all_assets
from src.validation import validate_bronze_csv, save_silver_dataframe
from src.storage import insert_silver_dataframe, get_db_engine
from src.gold_metrics import run_gold_layer
from src.logger import get_logger

logger = get_logger(__name__)   # ← THIS WAS MISSING

BRONZE_DIR = Path("data/bronze")


def run_pipeline() -> None:
    logger.info("Pipeline started")

    end_date = datetime.now(timezone.utc).date().isoformat()

    # Ingestion
    try:
        ingest_all_assets(
            start_date="2020-01-01",
            end_date=end_date,
        )
        logger.info("Ingestion step completed")
    except Exception as exc:
        logger.error("Ingestion step failed", exc_info=exc)
        return

    # Validation + Silver storage
    bronze_files = list(BRONZE_DIR.glob("*.csv"))

    if not bronze_files:
        logger.warning("No bronze files found, skipping silver processing")
    else:
        for bronze_file in bronze_files:
            logger.info("Processing bronze file", extra={"file": bronze_file.name})

            try:
                silver_df = validate_bronze_csv(bronze_file)

                if silver_df.empty:
                    logger.warning(
                        "No valid rows found",
                        extra={"file": bronze_file.name},
                    )
                    continue

                save_silver_dataframe(silver_df, bronze_file)
                insert_silver_dataframe(silver_df)

                logger.info(
                    "Silver data stored successfully",
                    extra={"file": bronze_file.name},
                )

            except Exception as exc:
                logger.error(
                    "Failed processing bronze file",
                    exc_info=exc,
                    extra={"file": bronze_file.name},
                )

    # Gold layer
    try:
        logger.info("Running gold layer")
        run_gold_layer()
        logger.info("Gold layer completed")
    except Exception as exc:
        logger.error("Gold layer failed", exc_info=exc)

    logger.info("Pipeline finished")


if __name__ == "__main__":
    run_pipeline()
