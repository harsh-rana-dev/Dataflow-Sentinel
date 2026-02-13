import logging
import os
import json
from pathlib import Path
from typing import Optional


class JsonFormatter(logging.Formatter):
    """
    Formats log records into JSON for cloud-native observability.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_record = {
            "timestamp": self.formatTime(record, "%Y-%m-%d %H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Inject run_id if present
        if hasattr(record, "run_id"):
            log_record["run_id"] = record.run_id

        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_record)


class RunIdFilter(logging.Filter):
    """
    Injects run_id into every log record.
    """

    def __init__(self, run_id: Optional[str] = None):
        super().__init__()
        self.run_id = run_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = self.run_id
        return True


def get_logger(
    name: str = "pipeline",
    log_dir: Path = Path("logs"),
    run_id: Optional[str] = None,
) -> logging.Logger:
    """
    Configures a dual-handler logger for console output and JSON file storage.
    """

    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.setLevel(level)

    # Console Handler (Human Readable)
    ch = logging.StreamHandler()
    ch.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    )

    # File Handler (Machine Readable JSON)
    log_dir.mkdir(exist_ok=True)
    fh = logging.FileHandler(log_dir / "pipeline.json")
    fh.setFormatter(JsonFormatter())

    # Inject run_id
    logger.addFilter(RunIdFilter(run_id))

    logger.addHandler(ch)
    logger.addHandler(fh)
    logger.propagate = False

    return logger
