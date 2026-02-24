import logging
import os
import json
from pathlib import Path
from typing import Optional

class JsonFormatter(logging.Formatter):
    # Class-level variable to store the run_id globally across all instances
    _GLOBAL_RUN_ID: Optional[str] = None

    def format(self, record: logging.LogRecord) -> str:
        log_record = {
            "timestamp": self.formatTime(record, "%Y-%m-%d %H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            # Use the global ID if the specific record doesn't have one
            "run_id": getattr(record, "run_id", self._GLOBAL_RUN_ID)
        }

        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_record)

def get_logger(
    name: str = "pipeline",
    log_dir: Path = Path("logs"),
    run_id: Optional[str] = None,
) -> logging.Logger:
    
    # If a run_id is provided, update the global state
    if run_id:
        JsonFormatter._GLOBAL_RUN_ID = run_id

    logger = logging.getLogger(name)

    # If logger already initialized, just update level/ID and return
    if logger.handlers:
        return logger

    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.setLevel(level)

    # Console Handler
    ch = logging.StreamHandler()
    ch.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    )

    # File Handler
    log_dir.mkdir(exist_ok=True)
    fh = logging.FileHandler(log_dir / "pipeline.json")
    fh.setFormatter(JsonFormatter())

    logger.addHandler(ch)
    logger.addHandler(fh)
    logger.propagate = False

    return logger