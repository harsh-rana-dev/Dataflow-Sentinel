import yaml
from datetime import datetime, timezone
from pathlib import Path
from src.ingestion import ingest_all_assets
from src.validation import validate_bronze_csv, save_silver_dataframe
from src.storage import insert_silver_dataframe
from src.gold_metrics import run_gold_layer
from src.logger import get_logger

# # Initializes the logger for the pipeline execution
logger = get_logger(__name__)

# Set Project Root and Config Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "assets.yaml"

# # Loads the YAML configuration file from the config directory
def load_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

config = load_config()

# Define Paths and Assets from Config as Path objects
BRONZE_DIR = PROJECT_ROOT / config['paths']['bronze']
SILVER_DIR = PROJECT_ROOT / config['paths']['silver']
GOLD_DIR = PROJECT_ROOT / config['paths']['gold']
TICKERS = config['assets']

# # Manages the end-to-end data flow from Ingestion to Gold layer
def run_pipeline() -> None:
    logger.info("Pipeline execution started")

    # Dynamic Date Logic
    start_date = config.get('start_date', "2024-01-01")
    end_date = config.get('end_date')
    if not end_date:
        end_date = datetime.now(timezone.utc).date().isoformat()

    logger.info(f"Context: {len(TICKERS)} assets | Timeframe: {start_date} to {end_date}")

    # --- 1. INGESTION (BRONZE) ---
    try:
        # # Triggers the fetching of data for all tickers defined in YAML
        ingest_all_assets(
            tickers=TICKERS,
            start_date=start_date,
            end_date=end_date,
            bronze_dir=BRONZE_DIR
        )
        logger.info("Bronze layer ingestion completed")
    except Exception as exc:
        logger.error("Ingestion critical failure", exc_info=exc)
        return

    # --- 2. VALIDATION & PROCESSING (SILVER) ---
    bronze_files = list(BRONZE_DIR.glob("*.csv"))

    if not bronze_files:
        logger.warning(f"No raw files found in {BRONZE_DIR}")
    else:
        for bronze_file in bronze_files:
            try:
                # # Cleans and validates the raw CSV data from the Bronze layer
                silver_df = validate_bronze_csv(bronze_file)

                if silver_df.empty:
                    continue

                # # Saves validated data to local storage and remote database
                save_silver_dataframe(silver_df, bronze_file, SILVER_DIR)
                insert_silver_dataframe(silver_df)
                
                logger.info(f"Processed {bronze_file.name} to Silver & Database")

            except Exception as exc:
                logger.error(f"Failed processing {bronze_file.name}", exc_info=exc)

    # --- 3. ANALYTICS (GOLD) ---
    try:
        # # Calculates business metrics and stores final results in the Gold layer
        run_gold_layer(silver_dir=SILVER_DIR, gold_dir=GOLD_DIR)
        logger.info("Gold layer analytics completed")
    except Exception as exc:
        logger.error("Gold layer failure", exc_info=exc)

    logger.info("Pipeline execution finished successfully")

if __name__ == "__main__":
    run_pipeline()