"""
================================================================================
FEATURE STORE INGESTION JOB — [Databricks Feature Engineering in UC]
================================================================================
Purpose: Scheduled job that computes features from upstream tables (e.g. Gold)
         and writes them to a UC feature table. Supports:
           • Batch mode (compute all / lookback window, then fe.write_table)
           • Streaming mode (readStream → compute → fe.write_table with streaming df)
         Freshness guard: skip if the feature table is already up-to-date.

Verified API (docs.databricks.com):
    fe.write_table(name=..., df=..., mode="merge")   # batch DataFrame
    fe.write_table(name=..., df=streaming_df, mode="merge")  # returns StreamingQuery

Key design choices (cost-effective, no Photon dependency):
    - Spark AQE enabled (auto coalesce/skew handling)
    - No `.count()` for early-exit (use `df.head(1)`)
    - `optimizeWrite.enabled=true` (fewer small files)
    - `mode='merge'` by default (upsert by PK — idempotent)
    - Freshness guard via table history (DESCRIBE HISTORY) to skip no-op runs

Customize (CHANGE_ME):
    - SOURCE_TABLE / _compute_features(): your feature computation logic
    - FEATURE_TABLE: UC 3-level name (must already have PK; create via creation/feature_group.py)
    - MODE: "merge" (incremental upsert, default) | "overwrite" (backfill)
    - USE_STREAMING: True for Autoloader / readStream; False for batch

Platform notes:
    - DBR 13.3 LTS ML+ (or pip install databricks-feature-engineering on non-ML).
    - Works on standard clusters (no Photon required; Photon accelerates if available).
    - AWS twin: aws/src/feature_store/ingestion/feature_store_job.py (FeatureStoreManager Spark connector).
Version : 2026-06-29
================================================================================
"""
import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger("feature_store_ingest_databricks")
spark = SparkSession.builder.getOrCreate()


class FeatureStoreIngestJob:
    # ---- CHANGE_ME ----
    SOURCE_TABLE: str = "main.gold.sales_mart"           # upstream source
    FEATURE_TABLE: str = "main.features.sales_features"  # UC feature table (must have PK)
    MODE: str = "merge"                                  # "merge" | "overwrite"
    USE_STREAMING: bool = False                          # True for streaming ingestion
    LOOKBACK_MONTHS: int = 3                             # 0 = all data
    CHECKPOINT_PATH: Optional[str] = None                # required if USE_STREAMING=True

    def __init__(self, cfg: Optional[dict] = None):
        if cfg:
            for k, v in cfg.items():
                if hasattr(self, k.upper()):
                    setattr(self, k.upper(), v)
        self._configure_spark()
        self._fe = None

    @property
    def fe(self):
        if self._fe is None:
            from databricks.feature_engineering import FeatureEngineeringClient
            self._fe = FeatureEngineeringClient()
        return self._fe

    def _configure_spark(self):
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

    # ---- freshness guard (skip if nothing new) ----
    def _is_fresh(self) -> bool:
        """Check if the feature table was updated AFTER the source's last commit.
        If yes, no new data to process — skip."""
        try:
            from delta.tables import DeltaTable
            src_ts = DeltaTable.forName(spark, self.SOURCE_TABLE).history(1).select("timestamp").collect()[0][0]
            tgt_ts = DeltaTable.forName(spark, self.FEATURE_TABLE).history(1).select("timestamp").collect()[0][0]
            if tgt_ts >= src_ts:
                logger.info(f"feature table is fresh (tgt={tgt_ts} >= src={src_ts}) — skip")
                return True
        except Exception as e:
            logger.info(f"freshness check skipped ({e})")
        return False

    # ---- feature computation (CHANGE_ME) ----
    def _compute_features(self, source_df: DataFrame) -> DataFrame:
        """Your feature computation logic. Override this.
        Must return a DataFrame that matches the feature table schema + PKs."""
        # EXAMPLE: simple aggregation
        return (source_df
                .groupBy("partner_code", "product")       # CHANGE_ME
                .agg(F.sum("daily_ga").alias("total_ga"),
                     F.avg("daily_ga").alias("avg_ga_30d"))
                )

    # ---- read source ----
    def _read_source(self) -> DataFrame:
        if self.USE_STREAMING:
            return spark.readStream.table(self.SOURCE_TABLE)
        df = spark.table(self.SOURCE_TABLE)
        if self.LOOKBACK_MONTHS > 0:
            # filter to recent months for incremental (assumes mnth_id column)
            from datetime import datetime
            from dateutil.relativedelta import relativedelta
            cutoff = int((datetime.now() - relativedelta(months=self.LOOKBACK_MONTHS)).strftime("%Y%m"))
            df = df.filter(F.col("mnth_id") >= cutoff)
            logger.info(f"source filtered: mnth_id >= {cutoff}")
        return df

    # ---- write ----
    def _write_features(self, features_df: DataFrame):
        """Write to the UC feature table via FeatureEngineeringClient."""
        self.fe.write_table(name=self.FEATURE_TABLE, df=features_df, mode=self.MODE)
        logger.info(f"write_table → {self.FEATURE_TABLE} (mode={self.MODE})")

    # ---- run ----
    def run(self):
        try:
            # Freshness guard (batch only)
            if not self.USE_STREAMING and self._is_fresh():
                return

            source = self._read_source()

            if not self.USE_STREAMING:
                # Batch: early-exit if source is empty
                if len(source.head(1)) == 0:
                    logger.info("source empty — nothing to ingest")
                    return
                features = self._compute_features(source)
                self._write_features(features)
            else:
                # Streaming: compute in each micro-batch, write as streaming df
                if not self.CHECKPOINT_PATH:
                    raise ValueError("USE_STREAMING=True requires CHECKPOINT_PATH")
                features = self._compute_features(source)
                # write_table with a streaming df returns a StreamingQuery
                q = self.fe.write_table(name=self.FEATURE_TABLE, df=features, mode=self.MODE)
                # If write_table does not natively handle streaming, use writeStream:
                if q is None:
                    (features.writeStream
                        .format("delta")
                        .option("checkpointLocation", self.CHECKPOINT_PATH)
                        .option("mergeSchema", "true")
                        .trigger(availableNow=True)
                        .toTable(self.FEATURE_TABLE))
                logger.info("streaming feature ingest started (availableNow)")

            logger.info("feature store ingestion complete")
        except Exception as e:
            logger.error(f"feature store ingestion failed: {e}")
            raise


# ============================================================================
# EXAMPLE (delete + replace)
# ============================================================================
if __name__ == "__main__":
    cfg = {
        "source_table": "main.gold.sales_mart",          # CHANGE_ME
        "feature_table": "main.features.sales_features",  # CHANGE_ME
        "mode": "merge",
        "use_streaming": "false",
        "lookback_months": "3",
    }
    FeatureStoreIngestJob(cfg).run()
