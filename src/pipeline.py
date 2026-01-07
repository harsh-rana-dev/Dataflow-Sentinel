from datetime import datetime, timezone

from ingestion import ingest_all_assets


def run_pipeline() -> None:
    """Main pipeline orchestration."""
    end_date = datetime.now(timezone.utc).date().isoformat()

    ingest_all_assets(
        start_date="2020-01-01",
        end_date=end_date,
    )


if __name__ == "__main__":
    run_pipeline()
