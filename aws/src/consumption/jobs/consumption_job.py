"""
================================================================================
CONSUMPTION LAYER TEMPLATE — Gold → Consumption (Reporting)
================================================================================
Purpose: Final transformation for reporting/BI consumption.
         Handles: cross-domain joins, zero-fill skeleton, Redshift-ready output.

Patterns:
    - Zero-fill with dimension table (48K sites × days/month)
    - INITIAL_LOAD vs daily incremental (controlled via DDB params)
    - Merge with existing data (append new, overwrite processed months)
    - Final column rename/reorder for BI tool compatibility

Args:
    --TARGET_BUCKET, --TARGET_DATABASE, --TARGET_TABLE
    --GOLD_DATABASE, --GOLD_TABLE
    --PARTITION_COLUMN, --INITIAL_LOAD (true/false), --LOOKBACK_DAYS
================================================================================
"""
import sys
import logging
from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("consumption_etl")


class BaseConsumptionJob:
    """
    Base class for Consumption ETL jobs. Override:
        - _define_gold_sources(): which gold tables to read
        - _define_dimension(): dimension table for zero-fill
        - _transform(gold_df, dim_df): final transformations
        - _get_metric_columns(): columns to zero-fill with 0
    """

    def __init__(self):
        self.args = getResolvedOptions(sys.argv, [
            "JOB_NAME", "TARGET_BUCKET", "TARGET_DATABASE", "TARGET_TABLE",
            "GOLD_DATABASE", "GOLD_TABLE", "PARTITION_COLUMN",
            "INITIAL_LOAD", "LOOKBACK_DAYS", "data_date",
        ])
        sc = SparkContext()
        self.glueContext = GlueContext(sc)
        self.spark = self.glueContext.spark_session
        self.job = Job(self.glueContext)
        self.job.init(self.args["JOB_NAME"], self.args)
        self._configure_spark()

    def _configure_spark(self):
        configs = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
            "spark.sql.shuffle.partitions": "400",
        }
        for k, v in configs.items():
            self.spark.conf.set(k, v)

    # ------------------------------------------------------------------
    # OVERRIDE THESE
    # ------------------------------------------------------------------

    def _define_gold_sources(self) -> dict:
        """Return dict: {"db": "...", "table": "..."}"""
        return {
            "db": self.args["GOLD_DATABASE"],
            "table": self.args["GOLD_TABLE"],
        }

    def _define_dimension(self) -> Optional[dict]:
        """Return dimension config for zero-fill, or None to skip.
        Example: {"db": "silver_db", "table": "cellsite_mapping", "key_col": "std_site_nm"}
        """
        return None

    def _get_metric_columns(self) -> List[str]:
        """Columns to fill with 0 for zero-fill skeleton. Override this."""
        return []

    def _transform(self, gold_df: DataFrame, dim_df: Optional[DataFrame]) -> DataFrame:
        """Final transformation. Override this."""
        return gold_df

    # ------------------------------------------------------------------
    # CORE LOGIC
    # ------------------------------------------------------------------

    def _read_gold(self) -> DataFrame:
        src = self._define_gold_sources()
        initial_load = self.args.get("INITIAL_LOAD", "false").lower() == "true"

        if initial_load:
            logger.info("INITIAL_LOAD=true — reading all gold data")
            return self.spark.sql(f"SELECT * FROM {src['db']}.{src['table']}")
        else:
            lookback = int(self.args.get("LOOKBACK_DAYS", "60"))
            logger.info(f"Incremental — lookback {lookback} days")
            # Compute partition filter
            from datetime import datetime, timedelta
            data_date = self.args.get("data_date", datetime.now().strftime("%Y-%m-%d"))[:10]
            cutoff = (datetime.strptime(data_date, "%Y-%m-%d") - timedelta(days=lookback))
            cutoff_partition = int(cutoff.strftime("%Y%m"))
            return self.spark.sql(
                f"SELECT * FROM {src['db']}.{src['table']} WHERE {self.args['PARTITION_COLUMN']} >= {cutoff_partition}"
            )

    def _read_dimension(self) -> Optional[DataFrame]:
        dim_config = self._define_dimension()
        if not dim_config:
            return None

        # Read latest partition of dimension table
        partitions = self.spark.sql(f"SHOW PARTITIONS {dim_config['db']}.{dim_config['table']}").collect()
        latest = sorted(r[0].split("=")[1] for r in partitions)[-1]
        part_col = partitions[0][0].split("=")[0]

        dyf = self.glueContext.create_dynamic_frame.from_catalog(
            database=dim_config["db"], table_name=dim_config["table"],
            push_down_predicate=f"{part_col} = '{latest}'"
        )
        key_col = dim_config["key_col"]
        df = dyf.toDF().filter(F.col(key_col).isNotNull()).select(key_col).dropDuplicates([key_col])
        logger.info(f"Dimension loaded: {df.count()} entities from partition {latest}")
        return df

    def _zero_fill(self, fact_df: DataFrame, dim_df: DataFrame, key_col: str) -> DataFrame:
        """Zero-fill: every dimension entity appears for every date in data."""
        dates = fact_df.select("data_dt", "mnth_id").distinct()
        skeleton = dim_df.crossJoin(F.broadcast(dates))

        result = skeleton.join(
            fact_df, on=[key_col, "data_dt"], how="left"
        )
        # Fill nulls with 0 for metric columns
        metric_cols = self._get_metric_columns()
        if metric_cols:
            result = result.fillna(0, subset=metric_cols)
        return result

    def _write_output(self, df: DataFrame):
        from aws.src.common.utils.etl_utils import DataOptimizer

        target_path = f"s3://{self.args['TARGET_BUCKET']}/{self.args['TARGET_TABLE']}"
        partition_col = self.args["PARTITION_COLUMN"]

        # --- FILE SIZING decision (§3 of PARTITIONING_FILE_SIZING_AND_TABLE_FORMATS) ---
        # Right-size output to ~256 MB/file to avoid the small-files problem.
        # No-op for delta/iceberg/databricks; coalesces/repartitions for plain parquet.
        # Pass row_count (e.g. from job_optimizer) to size by bytes without a full .count().
        df = DataOptimizer.right_size_output(df, platform="spark", row_count=None)

        logger.info(f"Writing consumption to {target_path}")
        (df.write.mode("overwrite")
         .partitionBy(partition_col)
         .format("parquet")
         .option("path", target_path)
         .saveAsTable(f"{self.args['TARGET_DATABASE']}.{self.args['TARGET_TABLE']}"))

    def run(self):
        try:
            gold_df = self._read_gold()
            dim_df = self._read_dimension()
            result = self._transform(gold_df, dim_df)
            self._write_output(result)
            self.job.commit()
            logger.info("Consumption job completed successfully")
        except Exception as e:
            logger.error(f"Consumption job failed: {e}", exc_info=True)
            raise


if __name__ == "__main__":
    BaseConsumptionJob().run()
