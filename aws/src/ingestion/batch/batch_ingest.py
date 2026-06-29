"""
================================================================================
BATCH INGESTION — external source → Bronze  [AWS Glue]
================================================================================
Purpose: Pull a batch source (S3 files OR JDBC database) into the Bronze landing
         zone. Thin wrapper that feeds the Bronze job; keeps source-connection
         concerns (JDBC, partitioned reads, secrets) in one place.

Sources:
    • s3   — files on S3 (parquet/csv/json)
    • jdbc — relational DB via JDBC (credentials from Secrets Manager)

Customize: SOURCE_TYPE, SOURCE_PATH / JDBC_*, TARGET_PATH/TABLE.
Glue params: --extra-jars <jdbc-driver.jar> for jdbc sources.
Databricks twin: databricks/src/ingestion/batch/batch_ingest.py
Version : 2026-06-28
================================================================================
"""
import sys
import json
import logging
from typing import Tuple

import boto3
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("batch_ingest_aws")


class BatchIngestAWS:
    def __init__(self):
        self.args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_TYPE", "TARGET_PATH", "TARGET_TABLE"])
        for i, a in enumerate(sys.argv):
            if a.startswith("--") and i + 1 < len(sys.argv) and not sys.argv[i + 1].startswith("--"):
                self.args.setdefault(a[2:], sys.argv[i + 1])
        self.source_type = self.args["SOURCE_TYPE"].lower()       # s3 | jdbc
        self.target_path = self.args["TARGET_PATH"]
        self.target_table = self.args["TARGET_TABLE"]
        self.region = self.args.get("REGION", "ap-southeast-1")
        sc = SparkContext.getOrCreate()
        self.gc = GlueContext(sc)
        self.spark = self.gc.spark_session
        self.job = Job(self.gc)
        self.job.init(self.args["JOB_NAME"], self.args)

    def _jdbc_creds(self, secret_id: str) -> Tuple[str, str]:
        sm = boto3.client("secretsmanager", region_name=self.region)
        s = json.loads(sm.get_secret_value(SecretId=secret_id)["SecretString"])
        return s["username"], s["password"]

    def read(self) -> DataFrame:
        if self.source_type == "s3":
            path = self.args["SOURCE_PATH"]                        # CHANGE_ME
            fmt = self.args.get("SOURCE_FORMAT", "parquet")
            logger.info(f"reading s3 {path} ({fmt})")
            r = self.spark.read.option("recursiveFileLookup", "true")
            if fmt == "csv":
                r = r.option("header", "true")
            return r.format(fmt).load(path)
        if self.source_type == "jdbc":
            user, pwd = self._jdbc_creds(self.args["JDBC_SECRET"])   # CHANGE_ME secret id
            logger.info(f"reading jdbc {self.args['JDBC_URL']} table={self.args['JDBC_TABLE']}")
            return (self.spark.read.format("jdbc")
                    .option("url", self.args["JDBC_URL"])            # CHANGE_ME
                    .option("dbtable", self.args["JDBC_TABLE"])       # CHANGE_ME
                    .option("user", user).option("password", pwd)
                    # TODO: for big tables add partitionColumn/numPartitions/lowerBound/upperBound
                    .load())
        raise SystemExit(f"Unsupported SOURCE_TYPE {self.source_type}")

    def write_bronze(self, df: DataFrame):
        df = (df.withColumn("_ingest_ts", F.current_timestamp())
                .withColumn("_ingest_date", F.date_format(F.current_date(), "yyyyMMdd")))
        (df.write.mode("append").partitionBy("_ingest_date")
           .format("parquet").option("path", self.target_path)
           .saveAsTable(self.target_table))
        logger.info(f"appended raw → {self.target_table}")

    def run(self):
        try:
            df = self.read()
            if len(df.head(1)) == 0:
                logger.info("no source rows — exit"); self.job.commit(); return
            self.write_bronze(df)
            self.job.commit()
            logger.info("batch ingest complete")
        except Exception as e:
            logger.error(f"batch ingest failed: {e}", exc_info=True); raise


if __name__ == "__main__":
    BatchIngestAWS().run()
