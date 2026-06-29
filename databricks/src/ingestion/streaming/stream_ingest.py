"""
================================================================================
STREAMING INGESTION — Autoloader / Kafka → Bronze  [Databricks]
================================================================================
Purpose: Twin of aws/src/ingestion/streaming/stream_ingest.py. Two modes:
    • autoloader — incremental cloud-file ingestion (cloudFiles), schema evolution
                   (recommended for files landing in S3/ADLS/GCS)
    • kafka      — Structured Streaming from Kafka

Checkpointing REQUIRED (exactly-once). Writes to a Bronze Delta table.

Customize: mode, source_path/kafka_*, target_table, checkpoint_path, schema_location.
Version : 2026-06-28
================================================================================
"""
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("stream_ingest_databricks")
spark = SparkSession.builder.getOrCreate()


class StreamIngestDatabricks:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.mode = cfg["mode"].lower()                    # autoloader | kafka
        self.target_table = cfg["target_table"]            # CHANGE_ME main.bronze.x
        self.checkpoint = cfg["checkpoint_path"]           # REQUIRED

    def read_stream(self) -> DataFrame:
        if self.mode == "autoloader":
            fmt = self.cfg.get("source_format", "parquet")
            logger.info(f"autoloader reading {self.cfg['source_path']} ({fmt})")
            r = (spark.readStream.format("cloudFiles")
                 .option("cloudFiles.format", fmt)
                 .option("cloudFiles.schemaLocation", self.cfg["schema_location"])  # CHANGE_ME
                 .option("cloudFiles.inferColumnTypes", "true"))
            if fmt == "csv":
                r = r.option("header", "true")
            return r.load(self.cfg["source_path"])         # CHANGE_ME
        if self.mode == "kafka":
            logger.info(f"kafka reading topic {self.cfg['kafka_topic']}")
            return (spark.readStream.format("kafka")
                    .option("kafka.bootstrap.servers", self.cfg["kafka_brokers"])   # CHANGE_ME
                    .option("subscribe", self.cfg["kafka_topic"])                    # CHANGE_ME
                    .option("startingOffsets", self.cfg.get("starting_offsets", "latest"))
                    .load()
                    .selectExpr("CAST(value AS STRING) AS raw_value", "timestamp AS kafka_ts"))
        raise SystemExit(f"Unsupported mode {self.mode}")

    def run(self):
        stream = (self.read_stream()
                  .withColumn("_ingest_ts", F.current_timestamp())
                  .withColumn("_ingest_date", F.date_format(F.current_date(), "yyyyMMdd")))
        q = (stream.writeStream
             .format("delta")
             .option("checkpointLocation", self.checkpoint)     # exactly-once
             .option("mergeSchema", "true")
             .partitionBy("_ingest_date")
             # availableNow=True → batch-drain mode; or use processingTime for continuous
             .trigger(availableNow=self.cfg.get("available_now", True))
             .toTable(self.target_table))
        logger.info("streaming started")
        q.awaitTermination()


if __name__ == "__main__":
    cfg = {"mode": "autoloader", "source_path": "s3://CHANGE_ME/raw/events/",
           "source_format": "json", "schema_location": "s3://CHANGE_ME/_schemas/events",
           "target_table": "main.bronze.events",
           "checkpoint_path": "s3://CHANGE_ME/_checkpoints/events"}
    StreamIngestDatabricks(cfg).run()
