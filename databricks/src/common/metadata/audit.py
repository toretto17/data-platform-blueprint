"""
================================================================================
AUDIT LOG WRITER — [Databricks]
================================================================================
Purpose: Twin of aws/src/common/metadata/audit.py. Same start()/finish() API.
         Sinks one audit row per run to a Delta audit table (queryable in
         Databricks SQL / time-travelable).

Usage:
    from databricks.src.common.metadata.audit import AuditLogger
    audit = AuditLogger(table="main.ops.etl_audit_log")
    run = audit.start("silver_sales", "silver", "main.bronze.sales", "main.silver.sales")
    audit.finish(run, rows_in=1000, rows_out=980, status="SUCCESS")
Version : 2026-06-28
================================================================================
"""
import logging
import time
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger("audit_databricks")
spark = SparkSession.builder.getOrCreate()


class AuditLogger:
    def __init__(self, table: str = "main.ops.etl_audit_log"):
        self.table = table
        self._ensure_table()

    def _ensure_table(self):
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                run_id STRING, job_name STRING, layer STRING, source STRING, target STRING,
                start_ts STRING, end_ts STRING, duration_sec DOUBLE,
                rows_in BIGINT, rows_out BIGINT, status STRING, error STRING
            ) USING DELTA
        """)

    def start(self, job_name: str, layer: str, source: str = "", target: str = "") -> dict:
        return {"run_id": str(uuid.uuid4()), "job_name": job_name, "layer": layer,
                "source": source, "target": target,
                "start_ts": datetime.now(timezone.utc).isoformat(), "_t0": time.time()}

    def finish(self, run: dict, rows_in: int = 0, rows_out: int = 0,
               status: str = "SUCCESS", error: str = ""):
        row = {
            "run_id": run["run_id"], "job_name": run["job_name"], "layer": run["layer"],
            "source": run.get("source", ""), "target": run.get("target", ""),
            "start_ts": run["start_ts"], "end_ts": datetime.now(timezone.utc).isoformat(),
            "duration_sec": round(time.time() - run.get("_t0", time.time()), 1),
            "rows_in": int(rows_in), "rows_out": int(rows_out),
            "status": status, "error": error[:1000],
        }
        (spark.createDataFrame([row])
              .write.format("delta").mode("append").saveAsTable(self.table))
        logger.info(f"AUDIT {row['job_name']} {status} rows_in={rows_in} rows_out={rows_out} "
                    f"dur={row['duration_sec']}s")
        return row
