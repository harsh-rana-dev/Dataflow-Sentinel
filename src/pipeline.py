import yaml
from datetime import datetime, timezone
from pathlib import Path

# --- Monitoring ---
from src.monitoring import init_monitoring, set_run_context
import sentry_sdk

# --- Pipeline Modules ---
from src.ingestion import ingest_all_assets
from src.validation import validate_bronze_csv, save_silver_dataframe
from src.storage import insert_silver_dataframe
from src.gold_metrics import run_gold_layer
from src.logger import get_logger


# Initialize monitoring as early as possible
init_monitoring()


PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "assets.yaml"
STATE_FILE = PROJECT_ROOT / "logs" / "pipeline_state.txt"


def load_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def load_processed_files() -> set:
    if not STATE_FILE.exists():
        return set()

    with open(STATE_FILE, "r") as f:
        return {line.strip() for line in f if line.strip()}


def save_processed_file(filename: str) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "a") as f:
        f.write(filename + "\n")


config = load_config()

BRONZE_DIR = PROJECT_ROOT / config["paths"]["bronze"]
SILVER_DIR = PROJECT_ROOT / config["paths"]["silver"]
GOLD_DIR = PROJECT_ROOT / config["paths"]["gold"]
TICKERS = config["assets"]


def run_pipeline() -> None:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    set_run_context(run_id)

    logger = get_logger(__name__, run_id=run_id)

    logger.info("Pipeline execution started")
    logger.info(f"Run ID: {run_id}")

    start_date = config.get("start_date", "2024-01-01")
    end_date = config.get("end_date") or datetime.now(timezone.utc).date().isoformat()

    logger.info(
        f"Context: {len(TICKERS)} assets | Timeframe: {start_date} to {end_date}"
    )

    try:
        # ---------------- INGESTION ----------------
        ingest_all_assets(
            tickers=TICKERS,
            start_date=start_date,
            end_date=end_date,
            bronze_dir=BRONZE_DIR,
            run_id=run_id,
        )
        logger.info("Bronze layer ingestion completed")

        # ---------------- SILVER ----------------
        processed_files = load_processed_files()
        bronze_files = sorted(BRONZE_DIR.glob(f"*_{run_id}.csv"))

        if not bronze_files:
            logger.warning("No raw files found for this run_id")
        else:
            new_data_processed = False

            for bronze_file in bronze_files:
                if bronze_file.name in processed_files:
                    logger.info(f"Skipping already processed file: {bronze_file.name}")
                    continue

                try:
                    silver_df = validate_bronze_csv(bronze_file)

                    if silver_df.empty:
                        logger.info(f"No valid data in {bronze_file.name}")
                        save_processed_file(bronze_file.name)
                        continue

                    save_silver_dataframe(silver_df, bronze_file, SILVER_DIR)
                    insert_silver_dataframe(silver_df)

                    save_processed_file(bronze_file.name)
                    new_data_processed = True

                    logger.info(f"Processed {bronze_file.name}")

                except Exception as exc:
                    logger.error(
                        f"Failed processing {bronze_file.name}",
                        exc_info=exc,
                    )
                    sentry_sdk.capture_exception(exc)

            # ---------------- GOLD ----------------
            if new_data_processed:
                run_gold_layer(
                    silver_dir=SILVER_DIR,
                    gold_dir=GOLD_DIR,
                    run_id=run_id,
                )
                logger.info("Gold layer analytics completed")
            else:
                logger.info("No new data processed — skipping Gold layer")

        # -------- SUCCESS MESSAGE --------
        logger.info("Pipeline execution finished successfully")
        sentry_sdk.capture_message(
            f"Pipeline run {run_id} completed successfully",
            level="info",
        )

    except Exception as exc:
        # -------- FAILURE MESSAGE --------
        logger.critical("Pipeline execution failed", exc_info=exc)
        sentry_sdk.capture_exception(exc)
        sentry_sdk.capture_message(
            f"Pipeline run {run_id} failed",
            level="error",
        )
        raise


if __name__ == "__main__":
    try:
        run_pipeline()
    finally:
        sentry_sdk.flush(timeout=5)