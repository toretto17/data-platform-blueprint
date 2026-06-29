"""
================================================================================
ETL JOB TEMPLATE — Silver Layer  [Databricks / Delta / Unity Catalog]
================================================================================
Purpose: Databricks twin of aws/src/silver/jobs/silver_job.py (Raw/Bronze → Silver).
         Same Base*Job shape and override points so logic is portable; only the
         engine differs (Delta + UC instead of Glue Catalog + Parquet).

Pattern:
    1. Read config (job params / widgets)
    2. Read source(s) (UC tables / Delta paths) with optional incremental filter
    3. Apply transformations (cleanse, cast, dedup)
    4. Derive columns (data_dt, mnth_id, ...)
    5. DQ checks (warn+skip via common dq_framework)
    6. Write to Silver Delta (append or dynamic-overwrite) — via get_writer()

Customize:
    - _define_sources()        : source tables/paths
    - _apply_transformations() : your cleansing/casting
    - _derive_columns()        : computed columns
    - _dq_config()             : DQ checks for this table

Platform notes: DBR 15.x LTS+, Delta 3.x, UC. AWS twin: aws/src/silver/jobs/silver_job.py
Version : 2026-06-28
================================================================================
"""
import logging
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Shared Databricks utils (same API as AWS tree)
from databricks.src.common.utils.etl_utils import EarlyExitCheck, get_writer
from databricks.src.common.validations.dq_framework import (
    DataQualityFramework, DQConfig, DQCheck, Severity,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("silver_etl_databricks")
spark = SparkSession.builder.getOrCreate()


class BaseSilverJobDatabricks:
    """Extend and override _define_sources / _apply_transformations / _derive_columns / _dq_config."""

    def __init__(self, cfg: dict):
        """cfg keys: target_table (UC), partition_col, mode (append|overwrite),
        lookback_days, data_date, source_system."""
        self.cfg = cfg
        self.target_table = cfg["target_table"]            # CHANGE_ME e.g. main.silver.sales
        self.partition_col = cfg.get("partition_col", "mnth_id")
        self.mode = cfg.get("mode", "overwrite")
        self._configure_spark()
        self.dq = DataQualityFramework(spark)
        logger.info(f"Silver(Databricks) init → {self.target_table} mode={self.mode}")

    def _configure_spark(self):
        spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        spark.conf.set("spark.sql.adaptive.enabled", "true")

    # ---- OVERRIDE ----
    def _define_sources(self) -> List[dict]:
        """Return [{"table": "main.bronze.sales"} | {"path": "s3://.../", "format":"delta"}]."""
        raise NotImplementedError("Override _define_sources()")

    def _apply_transformations(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError("Override _apply_transformations()")

    def _derive_columns(self, df: DataFrame) -> DataFrame:
        return df

    def _dq_config(self) -> DQConfig | None:
        return None

    # ---- CORE ----
    def _read_sources(self) -> DataFrame:
        frames = []
        for s in self._define_sources():
            if "table" in s:
                frames.append(spark.table(s["table"]))
            else:
                frames.append(spark.read.format(s.get("format", "delta")).load(s["path"]))
        df = frames[0]
        for extra in frames[1:]:
            df = df.unionByName(extra, allowMissingColumns=True)
        return df

    def run(self):
        try:
            raw = self._read_sources()
            if EarlyExitCheck.is_empty(raw):
                logger.info("No source data — early exit.")
                return
            df = self._apply_transformations(raw)
            df = self._derive_columns(df)
            if EarlyExitCheck.is_empty(df):
                logger.info("Transformations produced 0 rows — early exit.")
                return
            cfg = self._dq_config()
            if cfg:
                report = self.dq.validate(df, cfg)
                if report.has_failures:
                    self.dq.publish_metrics(report)
                    from databricks.src.common.exceptions.exceptions import DQError
                    raise DQError(report.summary)
            get_writer().write(df, self.target_table, partition_col=self.partition_col, mode=self.mode)
            logger.info("Silver(Databricks) job complete.")
        except Exception as e:
            logger.error(f"Silver(Databricks) job failed: {e}")
            raise


# ============================================================================
# EXAMPLE IMPLEMENTATION (delete + replace)
# ============================================================================
class SilverSalesJobDatabricks(BaseSilverJobDatabricks):
    def _define_sources(self):
        return [{"table": "main.bronze.sales"}]            # CHANGE_ME

    def _apply_transformations(self, df: DataFrame) -> DataFrame:
        return (df.filter(F.col("record_date").isNotNull())
                  .dropDuplicates(["transaction_id"])
                  .withColumn("amount", F.col("amount").cast(T.DecimalType(18, 2))))

    def _derive_columns(self, df: DataFrame) -> DataFrame:
        return (df.withColumn("data_dt", F.date_format("record_date", "yyyyMMdd").cast("int"))
                  .withColumn("mnth_id", F.date_format("record_date", "yyyyMM").cast("int")))

    def _dq_config(self):
        return DQConfig(table_name=self.target_table, checks=[
            DQCheck("rows", "row_count", Severity.CRITICAL, {"min_count": 1}),
            DQCheck("txn_id_not_null", "completeness", Severity.HIGH,
                    {"column": "transaction_id", "max_null_pct": 0.0}),
        ])


if __name__ == "__main__":
    # In Databricks, read these from dbutils.widgets.get(...)
    cfg = {"target_table": "main.silver.sales", "partition_col": "mnth_id", "mode": "overwrite"}
    SilverSalesJobDatabricks(cfg).run()
