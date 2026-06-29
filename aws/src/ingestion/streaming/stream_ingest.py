"""
================================================================================
STREAMING INGESTION — Kinesis / Kafka → Bronze  [AWS Glue Streaming]
================================================================================
Purpose: Consume a streaming source (Kinesis or Kafka/MSK) and append to Bronze
         (Parquet/Delta on S3) using Spark Structured Streaming with a checkpoint
         for exactly-once processing.

Sources: kinesis | kafka
Checkpointing is REQUIRED for fault tolerance/exactly-once — set CHECKPOINT_PATH.

Customize: SOURCE_TYPE, stream name/brokers/topic, TARGET_PATH/TABLE, CHECKPOINT_PATH.
Glue: create a Glue Streaming job (job type = Spark Streaming).
Databricks twin: databricks/src/ingestion/streaming/stream_ingest.py (Autoloader/Kafka)
Version : 2026-06-28
================================================================================
"""
import sys
import logging

from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("stream_ingest_aws")


class StreamIngestAWS:
    def __init__(self):
        self.args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_TYPE", "TARGET_PATH", "CHECKPOINT_PATH"])
        for i, a in enumerate(sys.argv):
            if a.startswith("--") and i + 1 < len(sys.argv) and not sys.argv[i + 1].startswith("--"):
                self.args.setdefault(a[2:], sys.argv[i + 1])
        self.source_type = self.args["SOURCE_TYPE"].lower()       # kinesis | kafka
        self.target_path = self.args["TARGET_PATH"]
        self.checkpoint = self.args["CHECKPOINT_PATH"]            # REQUIRED for exactly-once
        sc = SparkContext.getOrCreate()
        self.gc = GlueContext(sc)
        self.spark = self.gc.spark_session
        self.job = Job(self.gc)
        self.job.init(self.args["JOB_NAME"], self.args)

    def read_stream(self) -> DataFrame:
        if self.source_type == "kinesis":
            logger.info(f"reading kinesis {self.args.get('STREAM_NAME')}")
            return (self.spark.readStream.format("kinesis")
                    .option("streamName", self.args["STREAM_NAME"])     # CHANGE_ME
                    .option("startingPosition", self.args.get("STARTING_POSITION", "LATEST"))
                    .option("inferSchema", "true").load())
        if self.source_type == "kafka":
            logger.info(f"reading kafka topic {self.args.get('KAFKA_TOPIC')}")
            return (self.spark.readStream.format("kafka")
                    .option("kafka.bootstrap.servers", self.args["KAFKA_BROKERS"])  # CHANGE_ME
                    .option("subscribe", self.args["KAFKA_TOPIC"])                   # CHANGE_ME
                    .option("startingOffsets", self.args.get("STARTING_OFFSETS", "latest"))
                    .load()
                    # Kafka value is binary → cast to string (TODO: parse JSON/Avro to columns)
                    .selectExpr("CAST(value AS STRING) AS raw_value", "timestamp AS kafka_ts"))
        raise SystemExit(f"Unsupported SOURCE_TYPE {self.source_type}")

    def run(self):
        stream = self.read_stream().withColumn("_ingest_ts", F.current_timestamp())
        q = (stream.writeStream
             .format("parquet")                                   # or "delta"
             .option("path", self.target_path)
             .option("checkpointLocation", self.checkpoint)        # exactly-once
             .trigger(processingTime=self.args.get("TRIGGER", "60 seconds"))
             .start())
        logger.info("streaming started")
        q.awaitTermination()


if __name__ == "__main__":
    StreamIngestAWS().run()
