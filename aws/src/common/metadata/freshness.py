"""
================================================================================
FRESHNESS MANAGER — [AWS]
================================================================================
Purpose: Decide whether a job needs to run, by comparing the SOURCE's latest
         data point against the TARGET's. Avoids reprocessing when nothing new.

Two checks:
    • partition_freshness  : max partition value in a Glue table (source vs target)
    • s3_marker            : a small S3 text marker storing the last processed
                             watermark (works for any source)

Usage:
    fm = FreshnessManager(region="ap-southeast-1")
    src_max = fm.max_partition("bronze_db.sales", "mnth_id")
    if fm.marker_is_current("s3://bucket/_markers/silver_sales.txt", src_max):
        return  # skip — already processed
    ...do work...
    fm.write_marker("s3://bucket/_markers/silver_sales.txt", src_max)
Version : 2026-06-28
================================================================================
"""
import logging
from typing import Optional

import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger("freshness_aws")


class FreshnessManager:
    def __init__(self, spark: Optional[SparkSession] = None, region: str = "ap-southeast-1"):
        self.spark = spark or SparkSession.builder.getOrCreate()
        self.s3 = boto3.client("s3", region_name=region)

    def max_partition(self, table: str, partition_col: str) -> Optional[str]:
        """Latest partition value of a Glue table (string)."""
        v = self.spark.table(table).agg(F.max(partition_col)).collect()[0][0]
        return str(v) if v is not None else None

    # ---- S3 marker (portable watermark) ----
    def read_marker(self, s3_uri: str) -> Optional[str]:
        b, k = self._split(s3_uri)
        try:
            return self.s3.get_object(Bucket=b, Key=k)["Body"].read().decode().strip()
        except Exception:
            return None

    def write_marker(self, s3_uri: str, value: str):
        b, k = self._split(s3_uri)
        self.s3.put_object(Bucket=b, Key=k, Body=str(value).encode())
        logger.info(f"marker {s3_uri} = {value}")

    def marker_is_current(self, s3_uri: str, source_max: Optional[str]) -> bool:
        """True if the marker already covers source_max (→ skip processing)."""
        last = self.read_marker(s3_uri)
        return bool(last and source_max and last >= source_max)

    @staticmethod
    def _split(s3_uri: str):
        p = s3_uri.replace("s3://", "")
        return p.split("/")[0], "/".join(p.split("/")[1:])
