"""
================================================================================
FEATURE GROUP — create / describe / read  [AWS SageMaker Feature Store]
================================================================================
Purpose: Manage a SageMaker Feature Group (the AWS Feature Store unit). Create
         it (Iceberg offline store), describe/wait, and read features back for
         training/inference via Athena (offline store) — verified against the
         SageMaker Python SDK + boto3 docs.

What this does:
    • create_feature_group()   — idempotent create (Iceberg offline, online optional)
    • wait_until_created()
    • athena_query()           — read the offline store with SQL (PIT/training data)
    • get_offline_table()      — resolve the Glue table backing the FG

Ingestion (writing features) lives in ../ingestion/feature_store_job.py
(uses the Spark connector FeatureStoreManager for large Spark DataFrames).

Required FS columns: a record identifier (PK) + an event time column.

Connector/IAM (job params/role):
    IAM: sagemaker:CreateFeatureGroup, DescribeFeatureGroup, iam:PassRole,
         s3 on the offline bucket, glue + athena for reads.

Customize (CHANGE_ME): FG_NAME, RECORD_ID, EVENT_TIME, OFFLINE_S3_URI, ROLE_ARN.

Platform notes: SageMaker SDK v2 / boto3. Databricks twin:
    databricks/src/feature_store/creation/feature_group.py (UC feature table).
Version : 2026-06-28
================================================================================
"""
import time
import logging
from typing import List, Optional

import boto3

logger = logging.getLogger("feature_group_aws")


class FeatureGroupManager:
    def __init__(self, fg_name: str, record_id: str, event_time: str,
                 offline_s3_uri: str, role_arn: str, region: str = "ap-southeast-1"):
        self.fg_name = fg_name                  # CHANGE_ME
        self.record_id = record_id              # CHANGE_ME PK column
        self.event_time = event_time            # CHANGE_ME event-time column
        self.offline_s3_uri = offline_s3_uri    # CHANGE_ME s3://.../feature-store/<fg>
        self.role_arn = role_arn                # CHANGE_ME SageMaker exec role
        self.region = region
        self.sm = boto3.client("sagemaker", region_name=region)

    # ---- feature definitions from a Spark/pandas schema ----
    @staticmethod
    def _fs_type(spark_type_name: str) -> str:
        if spark_type_name in ("byte", "short", "integer", "int", "long", "bigint"):
            return "Integral"
        if spark_type_name in ("float", "double", "decimal"):
            return "Fractional"
        return "String"

    def feature_definitions_from_spark(self, df) -> List[dict]:
        """Build SageMaker FeatureDefinitions from a Spark DataFrame schema."""
        return [{"FeatureName": f.name, "FeatureType": self._fs_type(f.dataType.typeName())}
                for f in df.schema.fields]

    # ---- create (idempotent) ----
    def create_feature_group(self, feature_definitions: List[dict], enable_online: bool = False) -> str:
        """Create the FG with an Iceberg offline store. Returns the FG ARN.
        Idempotent: returns existing ARN if already Created."""
        try:
            d = self.sm.describe_feature_group(FeatureGroupName=self.fg_name)
            if d["FeatureGroupStatus"] == "Created":
                logger.info(f"FG exists: {self.fg_name}")
                return d["FeatureGroupArn"]
            if d["FeatureGroupStatus"] == "Creating":
                return self.wait_until_created()
        except self.sm.exceptions.ResourceNotFound:
            pass

        offline_cfg = {
            "S3StorageConfig": {"S3Uri": self.offline_s3_uri},
            "DisableGlueTableCreation": False,
            "TableFormat": "Iceberg",
        }
        kwargs = dict(
            FeatureGroupName=self.fg_name,
            RecordIdentifierFeatureName=self.record_id,
            EventTimeFeatureName=self.event_time,
            FeatureDefinitions=feature_definitions,
            RoleArn=self.role_arn,
            OfflineStoreConfig=offline_cfg,
        )
        if enable_online:
            kwargs["OnlineStoreConfig"] = {"EnableOnlineStore": True}
        self.sm.create_feature_group(**kwargs)
        logger.info(f"creating FG {self.fg_name} (Iceberg offline, online={enable_online})")
        return self.wait_until_created()

    def wait_until_created(self, timeout_s: int = 600) -> str:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            d = self.sm.describe_feature_group(FeatureGroupName=self.fg_name)
            st = d["FeatureGroupStatus"]
            if st == "Created":
                logger.info(f"FG created: {self.fg_name}")
                return d["FeatureGroupArn"]
            if st in ("CreateFailed", "Failed"):
                raise RuntimeError(f"FG create failed: {d.get('FailureReason')}")
            time.sleep(10)
        raise TimeoutError(f"FG {self.fg_name} not Created within {timeout_s}s")

    # ---- read offline store via Athena (training/inference data) ----
    def get_offline_table(self) -> tuple:
        """Return (glue_database, glue_table) backing the offline store."""
        dc = self.sm.describe_feature_group(
            FeatureGroupName=self.fg_name)["OfflineStoreConfig"]["DataCatalogConfig"]
        return dc.get("Database", "sagemaker_featurestore"), dc["TableName"]

    def athena_query(self, sql: str, output_s3: str, athena_workgroup: str = "primary") -> str:
        """Run an Athena query against the offline store; returns the result S3 path.
        Use this for point-in-time / training-set extraction (write your own SQL with
        ROW_NUMBER() OVER (PARTITION BY <record_id> ORDER BY <event_time> DESC) for PIT)."""
        athena = boto3.client("athena", region_name=self.region)
        q = athena.start_query_execution(
            QueryString=sql,
            ResultConfiguration={"OutputLocation": output_s3},
            WorkGroup=athena_workgroup)
        qid = q["QueryExecutionId"]
        while True:
            st = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
            if st in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(2)
        if st != "SUCCEEDED":
            raise RuntimeError(f"Athena query {st}")
        logger.info(f"athena query {qid} SUCCEEDED")
        return f"{output_s3.rstrip('/')}/{qid}.csv"

    def pit_training_sql(self, feature_cols: Optional[List[str]] = None) -> str:
        """Helper: build a point-in-time-deduped SELECT over the offline store
        (latest record per record_id). Customize joins/filters as needed."""
        db, table = self.get_offline_table()
        cols = ", ".join(feature_cols) if feature_cols else "*"
        return f"""
            SELECT {cols} FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY {self.record_id} ORDER BY {self.event_time} DESC, write_time DESC
                ) AS _rn
                FROM "{db}"."{table}"
                WHERE NOT is_deleted
            ) WHERE _rn = 1
        """


if __name__ == "__main__":
    # Example (needs a Spark df to derive feature definitions)
    mgr = FeatureGroupManager(
        fg_name="sales-features",                                   # CHANGE_ME
        record_id="record_id", event_time="event_time",
        offline_s3_uri="s3://CHANGE_ME/feature-store/sales-features",
        role_arn="arn:aws:iam::CHANGE_ME:role/SageMakerExecRole")
    # defs = mgr.feature_definitions_from_spark(df)
    # mgr.create_feature_group(defs)
    logger.info("see ../ingestion/feature_store_job.py to ingest features")
