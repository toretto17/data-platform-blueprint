"""
================================================================================
STRUCTURED LOGGER — [Databricks]
================================================================================
Purpose : Same API as the AWS logger (get_logger / log_metric) so DE/DS/MLOps
          code is identical across platforms — only this implementation differs.
Usage   :
    from databricks.src.common.logging.logger import get_logger
    logger = get_logger("silver_sales")
    logger.info("starting", extra={"rows": 123, "stage": "read"})

Platform notes:
    - Databricks routes Python logging to the driver log4j/stdout, visible in
      the cluster/job driver logs and (optionally) MLflow run tags.
    - For MLflow runs, log_metric also mirrors to mlflow.log_metric if a run is
      active (handy for ML jobs).
    - AWS twin: aws/src/common/logging/logger.py
Version : 2026-06-28
================================================================================
"""
import json
import logging
import sys
from typing import Any


class _KVFormatter(logging.Formatter):
    BASE = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"

    def __init__(self):
        super().__init__(self.BASE, datefmt="%Y-%m-%d %H:%M:%S")

    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        reserved = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime"}
        kv = {k: v for k, v in record.__dict__.items() if k not in reserved and not k.startswith("_")}
        if kv:
            try:
                base += " | " + " ".join(f"{k}={json.dumps(v, default=str)}" for k, v in kv.items())
            except Exception:
                base += " | " + str(kv)
        return base


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Return a configured singleton logger (driver stdout → Databricks job logs)."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_KVFormatter())
        logger.addHandler(handler)
        logger.setLevel(level)
        logger.propagate = False
    return logger


def log_metric(logger: logging.Logger, name: str, value: Any, **dims):
    """Log a metric line AND mirror to MLflow if a run is active (ML jobs)."""
    logger.info(f"METRIC {name}", extra={"metric_name": name, "metric_value": value, **dims})
    # Mirror numeric metrics to MLflow when inside an active run (no-op otherwise).
    try:
        import mlflow
        if mlflow.active_run() is not None and isinstance(value, (int, float)):
            mlflow.log_metric(name, float(value))
    except Exception:
        pass  # MLflow not present / no active run — logging still succeeded
