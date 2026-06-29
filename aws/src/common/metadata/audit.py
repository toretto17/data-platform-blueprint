"""
================================================================================
AUDIT LOG WRITER — [AWS]
================================================================================
Purpose: Record one audit row per job run (lineage + observability): run_id,
         job, layer, source/target, rows in/out, status, duration, error.
         Sinks to DynamoDB (fast point writes) or S3 (append parquet).

Why: every production pipeline needs an audit trail for debugging, SLAs, and
compliance ("what ran, when, how many rows, did it pass?").

Usage:
    from aws.src.common.metadata.audit import AuditLogger
    audit = AuditLogger(sink="dynamodb", table="etl_audit_log")
    run = audit.start(job_name="silver_sales", layer="silver",
                      source="bronze.sales", target="silver.sales")
    ...
    audit.finish(run, rows_in=1000, rows_out=980, status="SUCCESS")
    # on error: audit.finish(run, status="FAILED", error=str(e))
Version : 2026-06-28
================================================================================
"""
import json
import logging
import time
import uuid
from datetime import datetime, timezone

import boto3

logger = logging.getLogger("audit_aws")


class AuditLogger:
    def __init__(self, sink: str = "dynamodb", table: str = "etl_audit_log",
                 s3_path: str | None = None, region: str = "ap-southeast-1"):
        self.sink = sink                       # "dynamodb" | "s3"
        self.table = table                     # DynamoDB table (sink=dynamodb)
        self.s3_path = s3_path                 # s3://.../audit/ (sink=s3)
        self.region = region
        if sink == "dynamodb":
            self._ddb = boto3.client("dynamodb", region_name=region)

    def start(self, job_name: str, layer: str, source: str = "", target: str = "") -> dict:
        return {
            "run_id": str(uuid.uuid4()),
            "job_name": job_name,
            "layer": layer,
            "source": source,
            "target": target,
            "start_ts": datetime.now(timezone.utc).isoformat(),
            "_t0": time.time(),
        }

    def finish(self, run: dict, rows_in: int = 0, rows_out: int = 0,
               status: str = "SUCCESS", error: str = ""):
        run = {**run}
        run["rows_in"] = int(rows_in)
        run["rows_out"] = int(rows_out)
        run["status"] = status
        run["error"] = error[:1000]
        run["end_ts"] = datetime.now(timezone.utc).isoformat()
        run["duration_sec"] = round(time.time() - run.pop("_t0", time.time()), 1)
        self._write(run)
        logger.info(f"AUDIT {run['job_name']} {status} rows_in={rows_in} rows_out={rows_out} "
                    f"dur={run['duration_sec']}s")
        return run

    def _write(self, run: dict):
        if self.sink == "dynamodb":
            item = {k: {"S": str(v)} if not isinstance(v, (int, float)) else {"N": str(v)}
                    for k, v in run.items()}
            self._ddb.put_item(TableName=self.table, Item=item)
        elif self.sink == "s3":
            s3 = boto3.client("s3", region_name=self.region)
            bucket = self.s3_path.replace("s3://", "").split("/")[0]
            prefix = "/".join(self.s3_path.replace("s3://", "").split("/")[1:]).rstrip("/")
            key = f"{prefix}/dt={run['start_ts'][:10]}/{run['run_id']}.json"
            s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(run).encode())
        else:
            raise ValueError("sink must be dynamodb|s3")
