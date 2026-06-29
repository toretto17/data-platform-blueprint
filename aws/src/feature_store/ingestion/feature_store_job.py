"""
================================================================================
FEATURE STORE PRODUCER TEMPLATE — SageMaker Feature Store (Iceberg)
================================================================================
Purpose: Ingest Gold-layer features into SageMaker Feature Store via Spark connector.
         Supports backfill, incremental, and freshness-guarded ingestion.

Pattern:
    1. Freshness check (skip if FS already has latest Gold data)
    2. Read Gold table for target month window
    3. Add record_id + event_time (FS required columns)
    4. Ensure Feature Group exists (auto-create if not)
    5. Ingest via FeatureStoreManager Spark connector
    6. Update freshness marker

Prerequisites:
    - Glue job extra-jars: sagemaker-feature-store-spark-sdk-3.5.jar
    - IAM: DescribeFeatureGroup, PutRecord, GetRecord, BatchGetRecord
    - Feature Group pre-created or auto-create enabled

Args:
    --GOLD_DATABASE, --GOLD_TABLE, --FEATURE_BUCKET, --FS_PREFIX, --FG_NAME
    --TARGET_MONTH, --LOOKBACK_MONTHS, --BACKFILL (true/false)
    --ROLE_ARN, --FORCE_RUN, --REGION
================================================================================
"""
import sys
import logging
from datetime import datetime

import boto3
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from feature_store_pyspark.FeatureStoreManager import FeatureStoreManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("feature_store_producer")


class BaseFeatureStoreProducer:
    """
    Base class for Feature Store ingestion jobs. Override:
        - FEATURE_GROUP_NAME: str
        - RECORD_ID_COLUMNS: list — columns that form the unique record ID
        - _build_record_id(df): how to construct the record_id column
        - _select_features(df): which columns to ingest
    """

    FEATURE_GROUP_NAME = "CHANGE_ME-features"   # e.g., "sales-anomaly-features"
    RECORD_ID_COLUMNS = ["entity_id"]           # Columns forming unique ID
    EVENT_TIME_COLUMN = "event_time"            # Required by FS
    MONTH_COLUMN = "mnth_id"                    # Partition column in Gold

    def __init__(self):
        self.args = getResolvedOptions(sys.argv, [
            "JOB_NAME", "GOLD_DATABASE", "GOLD_TABLE",
            "FEATURE_BUCKET", "FS_PREFIX", "FG_NAME",
            "TARGET_MONTH", "LOOKBACK_MONTHS", "BACKFILL",
            "ROLE_ARN", "FORCE_RUN", "REGION",
        ])
        sc = SparkContext()
        self.glueContext = GlueContext(sc)
        self.spark = self.glueContext.spark_session
        self.job = Job(self.glueContext)
        self.job.init(self.args["JOB_NAME"], self.args)

        self.sm_client = boto3.client("sagemaker", region_name=self.args["REGION"])
        self.s3_client = boto3.client("s3", region_name=self.args["REGION"])
        self.fs_manager = FeatureStoreManager()

        self.fg_name = self.args.get("FG_NAME", self.FEATURE_GROUP_NAME)
        self.role_arn = self.args["ROLE_ARN"]

    # ------------------------------------------------------------------
    # OVERRIDE THESE
    # ------------------------------------------------------------------

    def _build_record_id(self, df):
        """Construct unique record_id column. Override for composite keys."""
        # Default: concatenate RECORD_ID_COLUMNS with '|'
        return df.withColumn("record_id",
                             F.concat_ws("|", *[F.col(c).cast("string") for c in self.RECORD_ID_COLUMNS]))

    def _select_features(self, df):
        """Select and rename columns for Feature Store. Override this."""
        return df  # Default: ingest all columns

    def _add_event_time(self, df):
        """Add event_time column (unix epoch float). Override if different logic."""
        return df.withColumn(self.EVENT_TIME_COLUMN,
                             F.unix_timestamp(F.current_timestamp()).cast("double"))

    # ------------------------------------------------------------------
    # CORE LOGIC
    # ------------------------------------------------------------------

    def _get_target_months(self) -> list:
        """Determine which months to ingest."""
        if self.args.get("BACKFILL", "false").lower() == "true":
            # Backfill: all months in Gold
            months_df = self.spark.sql(
                f"SELECT DISTINCT {self.MONTH_COLUMN} FROM {self.args['GOLD_DATABASE']}.{self.args['GOLD_TABLE']}"
            )
            return sorted([r[0] for r in months_df.collect()])

        # Incremental: target month ± lookback
        target = int(self.args.get("TARGET_MONTH", datetime.now().strftime("%Y%m")))
        lookback = int(self.args.get("LOOKBACK_MONTHS", "3"))
        # Simple: generate last N months
        months = []
        for i in range(lookback):
            m = target - i  # Simplified; handle year boundary in production
            months.append(m)
        return sorted(months)

    def _check_freshness(self) -> bool:
        """Check if FS already has latest data. Returns True if fresh (skip)."""
        if self.args.get("FORCE_RUN", "false").lower() == "true":
            return False  # Force = never skip
        # Check S3 marker
        marker_key = f"{self.args['FS_PREFIX']}/{self.fg_name}/_last_ingested_month"
        try:
            resp = self.s3_client.get_object(
                Bucket=self.args["FEATURE_BUCKET"], Key=marker_key)
            last_month = resp["Body"].read().decode().strip()
            target = self.args.get("TARGET_MONTH", "")
            if last_month >= target:
                logger.info(f"FS already has month {last_month} >= target {target}. Skipping.")
                return True
        except self.s3_client.exceptions.NoSuchKey:
            pass
        return False

    def _update_freshness_marker(self, latest_month: str):
        """Update S3 marker after successful ingestion."""
        marker_key = f"{self.args['FS_PREFIX']}/{self.fg_name}/_last_ingested_month"
        self.s3_client.put_object(
            Bucket=self.args["FEATURE_BUCKET"],
            Key=marker_key,
            Body=latest_month.encode(),
        )

    def _ensure_feature_group_exists(self, df):
        """Create Feature Group if it doesn't exist."""
        try:
            self.sm_client.describe_feature_group(FeatureGroupName=self.fg_name)
            logger.info(f"Feature Group '{self.fg_name}' exists")
        except self.sm_client.exceptions.ResourceNotFound:
            logger.info(f"Creating Feature Group '{self.fg_name}'...")
            # Build feature definitions from DataFrame schema
            feature_defs = []
            for field in df.schema.fields:
                ft = "String"
                if field.dataType in (T.IntegerType(), T.LongType()):
                    ft = "Integral"
                elif field.dataType in (T.FloatType(), T.DoubleType()):
                    ft = "Fractional"
                feature_defs.append({"FeatureName": field.name, "FeatureType": ft})

            self.sm_client.create_feature_group(
                FeatureGroupName=self.fg_name,
                RecordIdentifierFeatureName="record_id",
                EventTimeFeatureName=self.EVENT_TIME_COLUMN,
                FeatureDefinitions=feature_defs,
                OnlineStoreConfig={"EnableOnlineStore": False},
                OfflineStoreConfig={
                    "S3StorageConfig": {
                        "S3Uri": f"s3://{self.args['FEATURE_BUCKET']}/{self.args['FS_PREFIX']}/{self.fg_name}/"
                    },
                    "TableFormat": "Iceberg",
                },
                RoleArn=self.role_arn,
            )
            # Wait for creation
            import time
            for _ in range(30):
                status = self.sm_client.describe_feature_group(
                    FeatureGroupName=self.fg_name)["FeatureGroupStatus"]
                if status == "Created":
                    break
                time.sleep(10)

    def run(self):
        try:
            # 1. Freshness guard
            if self._check_freshness():
                self.job.commit()
                return

            # 2. Determine months
            months = self._get_target_months()
            logger.info(f"Ingesting months: {months}")

            # 3. Read Gold
            month_filter = " OR ".join([f"{self.MONTH_COLUMN} = {m}" for m in months])
            df = self.spark.sql(
                f"SELECT * FROM {self.args['GOLD_DATABASE']}.{self.args['GOLD_TABLE']} WHERE {month_filter}"
            )

            # 4. Add FS required columns
            df = self._build_record_id(df)
            df = self._add_event_time(df)
            df = self._select_features(df)

            # 5. Ensure FG exists
            self._ensure_feature_group_exists(df)

            # 6. Ingest
            logger.info(f"Ingesting {df.count()} records to '{self.fg_name}'")
            self.fs_manager.ingest_data(
                input_data_frame=df,
                feature_group_arn=f"arn:aws:sagemaker:{self.args['REGION']}:{self.args.get('ACCOUNT_ID', '')}:feature-group/{self.fg_name}",
                target_stores=["OfflineStore"],
            )

            # 7. Update marker
            self._update_freshness_marker(str(max(months)))

            self.job.commit()
            logger.info("Feature Store ingestion completed successfully")

        except Exception as e:
            logger.error(f"Feature Store job failed: {e}", exc_info=True)
            raise


if __name__ == "__main__":
    BaseFeatureStoreProducer().run()
