import time
import os

LOG_FILENAME = "app.log"

from datetime import datetime

def _timestamp() -> str:
    """Return current time in human readable format with milliseconds."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _write_log(level: str, message: str):
    """Append a log line to the file with timestamp and level."""
    line = f"{_timestamp()} [{level.upper()}] {message}\n"
    with open(LOG_FILENAME, "a", encoding="utf-8") as f:
        f.write(line)
    print(line, end="")  # optional: also print to console

# Simple log functions
def info(message: str):
    _write_log("INFO", message)

def warning(message: str):
    _write_log("WARNING", message)

def error(message: str):
    _write_log("ERROR", message)
