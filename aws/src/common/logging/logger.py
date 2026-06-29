"""
================================================================================
STRUCTURED LOGGER — [AWS Glue / CloudWatch]
================================================================================
Purpose : One consistent, structured (JSON-ish) logger for all AWS jobs so
          CloudWatch Logs Insights can filter/aggregate easily.
Usage   :
    from aws.src.common.logging.logger import get_logger
    logger = get_logger("silver_sales")
    logger.info("starting", extra={"rows": 123, "stage": "read"})

Why structured: CloudWatch Logs Insights can parse `key=value` / JSON fields,
so you can run queries like:  fields @timestamp, rows | filter stage="read"

Platform notes:
    - AWS: plain Python logging → stdout → CloudWatch (Glue ships stdout).
    - Databricks twin: databricks/src/common/logging/logger.py (log4j-aware).
Version : 2026-06-28
================================================================================
"""
import json
import logging
import sys
from typing import Any


class _KVFormatter(logging.Formatter):
    """Render the message plus any `extra` dict as compact key=value pairs."""

    BASE = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"

    def __init__(self):
        super().__init__(self.BASE, datefmt="%Y-%m-%d %H:%M:%S")

    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        # Anything attached via logger.info(..., extra={...}) lands as record attrs.
        reserved = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime"}
        kv = {k: v for k, v in record.__dict__.items() if k not in reserved and not k.startswith("_")}
        if kv:
            try:
                base += " | " + " ".join(f"{k}={json.dumps(v, default=str)}" for k, v in kv.items())
            except Exception:
                base += " | " + str(kv)
        return base


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Return a configured singleton logger. Safe to call repeatedly."""
    logger = logging.getLogger(name)
    if not logger.handlers:                       # avoid duplicate handlers on re-import
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_KVFormatter())
        logger.addHandler(handler)
        logger.setLevel(level)
        logger.propagate = False                  # don't double-log via root
    return logger


def log_metric(logger: logging.Logger, name: str, value: Any, **dims):
    """Emit a metric line you can later scrape in CloudWatch Logs Insights.
    Example: log_metric(logger, "rows_written", 12345, stage="gold", table="x")
    """
    logger.info(f"METRIC {name}", extra={"metric_name": name, "metric_value": value, **dims})
