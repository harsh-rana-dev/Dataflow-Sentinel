# logger.py
import logging
from pathlib import Path
import json

# ----------------------------
# 1️⃣ Create logs folder
# ----------------------------
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# ----------------------------
# 2️⃣ Custom JSON Formatter for file logs
# ----------------------------
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "time": self.formatTime(record, "%Y-%m-%d %H:%M:%S"),
            "level": record.levelname,
            "message": record.getMessage()
        }
        return json.dumps(log_record)

# ----------------------------
# 3️⃣ Create logger
# ----------------------------
logger = logging.getLogger("pipeline")
logger.setLevel(logging.INFO)  # Minimum level to capture

# ----------------------------
# 4️⃣ Console handler
# ----------------------------
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s",
                                      datefmt="%Y-%m-%d %H:%M:%S")
console_handler.setFormatter(console_formatter)

# ----------------------------
# 5️⃣ File handler
# ----------------------------
file_handler = logging.FileHandler(LOG_DIR / "pipeline_run.json")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(JsonFormatter())

# ----------------------------
# 6️⃣ Add handlers to logger
# ----------------------------
logger.addHandler(console_handler)
logger.addHandler(file_handler)
