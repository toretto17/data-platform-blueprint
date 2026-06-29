"""
================================================================================
GLUE ETL JOB TEMPLATE — Bronze Layer  [AWS Glue]
================================================================================
Purpose: Template for building Bronze-layer ETL jobs (Raw source → Bronze).
         Bronze = the raw landing zone. We keep data AS-IS (no business logic),
         add ingestion lineage columns, and append by ingest date so we retain
         full raw history. Cleansing/dedup happens later in Silver.

Pattern:
    1. Parse args from DDB config (via framework Step Function)
    2. Read raw source (S3 files OR cross-account Glue Catalog OR JDBC)
    3. Add ingestion audit columns (_ingest_ts, _ingest_date, _source_file)
    4. (Light) raw DQ — warn+skip, never crash
    5. Write to Bronze (append-only, partitioned by _ingest_date)

Customize (search "CHANGE_ME" / "TODO"):
    - _define_sources()        : where raw data comes from
    - _read_raw()              : source-specific read options (format/JDBC)
    - _add_audit_columns()     : lineage columns you want to stamp
    - _get_dq_ruleset_name()   : optional raw DQ ruleset

Args (from DDB config via framework):
    --TARGET_BUCKET, --TARGET_DATABASE, --TARGET_TABLE
    --PARTITION_COLUMN (default _ingest_date), --MODE (append for Bronze)
    --SOURCE_TYPE (s3|catalog|jdbc), --SOURCE_PATH / --SOURCE_DATABASE+--SOURCE_TABLE
    --DQ_BUCKET, --source_system
    Framework: --JOB_NAME, --data_date (dl_date from SF)

Platform notes:
    - AWS: Glue 5.x / Spark 3.5. JDBC needs the driver on --extra-jars.
    - Databricks twin: bronze_job_databricks.py (Autoloader → Delta).
Version : 2026-06-28 — matches BaseSilverJob / BaseGoldJob structure
================================================================================
"""
import sys
import json
import logging
from typing import Optional, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("bronze_etl")


# ============================================================================
# DATA QUALITY — warn+skip pattern (never crash on missing ruleset)
# ============================================================================
class DataQualityManager:
    """DQ validation that warns instead of crashing if rulesets are missing."""

    def __init__(self, glue_client, glue_context=None):
        self.glue_client = glue_client
        self.glue_context = glue_context   # needed for in-job EvaluateDataQuality

    def get_ruleset(self, ruleset_name: str) -> Optional[str]:
        try:
            response = self.glue_client.get_data_quality_ruleset(Name=ruleset_name)
            return str(response["Ruleset"])
        except self.glue_client.exceptions.EntityNotFoundException:
            logger.warning(f"DQ ruleset '{ruleset_name}' not found — skipping")
            return None
        except Exception as e:
            logger.warning(f"Failed to fetch ruleset '{ruleset_name}': {e} — skipping")
            return None

    def validate(self, df: DataFrame, ruleset_name: str) -> bool:
        """Run a Glue Data Quality ruleset against df. Returns True if passed or
        skipped (missing ruleset), False if a rule failed.
        Real implementation using the in-job EvaluateDataQuality transform."""
        ruleset_str = self.get_ruleset(ruleset_name)
        if ruleset_str is None:
            return True  # Skip = pass (warn already logged)
        try:
            from awsgluedq.transforms import EvaluateDataQuality
            from awsglue.dynamicframe import DynamicFrame

            dyf = DynamicFrame.fromDF(df, self.glue_context, "dq_input")
            outcomes = EvaluateDataQuality().process_rows(
                frame=dyf,
                ruleset=ruleset_str,
                publishing_options={
                    "dataQualityEvaluationContext": ruleset_name,
                    "enableDataQualityCloudWatchMetrics": True,
                    "enableDataQualityResultsPublishing": True,
                },
            )
            results = outcomes.errorsAsDynamicFrame().toDF()  # rule-level outcomes
            failed = [r for r in results.collect() if str(r.asDict().get("Outcome", "")).lower() == "failed"]
            if failed:
                for r in failed:
                    logger.error(f"DQ rule FAILED: {r.asDict().get('Rule')}")
                return False
            logger.info(f"DQ ruleset '{ruleset_name}' passed.")
            return True
        except ImportError:
            # awsgluedq not available (local/dev) → run a basic real fallback check.
            logger.warning("awsgluedq not available — running basic fallback DQ (row_count > 0).")
            return len(df.head(1)) > 0


# ============================================================================
# BASE BRONZE JOB
# ============================================================================
class BaseBronzeJob:
    """
    Base class for Bronze ETL jobs. Extend and override:
        - _define_sources()   -> source config(s)
        - _read_raw()         -> how to read your source (override only if non-standard)
        - _add_audit_columns()-> lineage columns (usually keep default)

    Bronze principle: DO NOT apply business logic here. Land raw + lineage only.
    """

    def __init__(self):
        # Required args — fail fast if missing (no silent fallback)
        self.args = getResolvedOptions(sys.argv, [
            "JOB_NAME",
            "TARGET_BUCKET",
            "TARGET_DATABASE",
            "TARGET_TABLE",
            "PARTITION_COLUMN",
            "MODE",
            "SOURCE_TYPE",
            "source_system",
            "data_date",
        ])
        # Optional args parsed permissively (SOURCE_PATH / SOURCE_DATABASE / etc.)
        for i, a in enumerate(sys.argv):
            if a.startswith("--") and i + 1 < len(sys.argv) and not sys.argv[i + 1].startswith("--"):
                self.args.setdefault(a[2:], sys.argv[i + 1])

        self.region = self.args.get("REGION", "ap-southeast-1")

        sc = SparkContext()
        self.glueContext = GlueContext(sc)
        self.spark = self.glueContext.spark_session
        self.job = Job(self.glueContext)
        self.job.init(self.args["JOB_NAME"], self.args)

        self._configure_spark()

        import boto3
        self.dq_manager = DataQualityManager(boto3.client("glue", region_name=self.region), self.glueContext)

        logger.info(f"Bronze job initialized: {self.args['JOB_NAME']}")
        logger.info(f"Target: {self.args['TARGET_DATABASE']}.{self.args['TARGET_TABLE']}")
        logger.info(f"Source type: {self.args['SOURCE_TYPE']}, Mode: {self.args['MODE']}")

    def _configure_spark(self):
        """Spark configs. Override for custom tuning."""
        configs = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
        }
        for k, v in configs.items():
            self.spark.conf.set(k, v)

    # ------------------------------------------------------------------
    # OVERRIDE THESE IN YOUR JOB
    # ------------------------------------------------------------------

    def _define_sources(self) -> List[dict]:
        """Define raw source(s). Override this.
        Returns list of dicts. Examples:
          s3:      [{"type":"s3","path":"s3://raw/.../","format":"parquet"}]
          catalog: [{"type":"catalog","db":"src_db","table":"raw_tbl"}]
          jdbc:    [{"type":"jdbc","url":"jdbc:...","table":"schema.tbl","secret":"sm-id"}]
        """
        raise NotImplementedError("Override _define_sources()")

    def _get_dq_ruleset_name(self) -> Optional[str]:
        """Optional raw DQ ruleset name. Return None to skip."""
        return None

    # ------------------------------------------------------------------
    # CORE LOGIC (usually no need to override)
    # ------------------------------------------------------------------

    def _get_jdbc_credentials(self, secret_id: str):
        import boto3
        sm = boto3.client("secretsmanager", region_name=self.region)
        secret = json.loads(sm.get_secret_value(SecretId=secret_id)["SecretString"])
        return secret["username"], secret["password"]

    def _read_raw(self) -> DataFrame:
        """Read raw source per its type. Override only for non-standard sources."""
        frames = []
        for src in self._define_sources():
            stype = src["type"]
            if stype == "s3":
                logger.info(f"Reading S3: {src['path']} ({src.get('format', 'parquet')})")
                reader = self.spark.read.option("recursiveFileLookup", "true")
                fmt = src.get("format", "parquet")
                if fmt == "csv":
                    reader = reader.option("header", "true")
                frames.append(reader.format(fmt).load(src["path"]))
            elif stype == "catalog":
                logger.info(f"Reading Catalog: {src['db']}.{src['table']}")
                dyf = self.glueContext.create_dynamic_frame.from_catalog(
                    database=src["db"], table_name=src["table"],
                    push_down_predicate=src.get("predicate"))
                frames.append(dyf.toDF())
            elif stype == "jdbc":
                user, pwd = self._get_jdbc_credentials(src["secret"])
                logger.info(f"Reading JDBC: {src['url']} table={src['table']}")
                frames.append(self.spark.read.format("jdbc")
                              .option("url", src["url"]).option("dbtable", src["table"])
                              .option("user", user).option("password", pwd).load())
            else:
                raise SystemExit(f"Unsupported source type: {stype}")
        # Single source returns df; multi-source unions (override if you need joins)
        df = frames[0]
        for extra in frames[1:]:
            df = df.unionByName(extra, allowMissingColumns=True)
        return df

    def _add_audit_columns(self, df: DataFrame) -> DataFrame:
        """Stamp ingestion lineage. CHANGE_ME to add more lineage columns."""
        df = (df
              .withColumn("_ingest_ts", F.current_timestamp())
              .withColumn("_ingest_date", F.date_format(F.current_date(), "yyyyMMdd"))
              .withColumn("_source_system", F.lit(self.args["source_system"])))
        # _source_file only available for file-based reads
        try:
            df = df.withColumn("_source_file", F.input_file_name())
        except Exception:
            df = df.withColumn("_source_file", F.lit(None).cast("string"))
        return df

    def _write_output(self, df: DataFrame):
        """Bronze write = append-only, partitioned by _ingest_date."""
        target_path = f"s3://{self.args['TARGET_BUCKET']}/{self.args['TARGET_TABLE']}"
        partition_col = self.args["PARTITION_COLUMN"]
        mode = self.args["MODE"]  # Bronze should be "append"
        logger.info(f"Writing Bronze → {target_path} (mode={mode}, partition={partition_col})")
        (df.write
           .mode(mode)
           .partitionBy(partition_col)
           .format("parquet")
           .option("path", target_path)
           .saveAsTable(f"{self.args['TARGET_DATABASE']}.{self.args['TARGET_TABLE']}"))
        logger.info("Bronze write complete.")

    def run(self):
        """Execute the Bronze ingestion pipeline."""
        from aws.src.common.utils.etl_utils import EarlyExitCheck

        try:
            # 1. Read raw
            raw_df = self._read_raw()

            # 2. EARLY EXIT — no new raw data
            if EarlyExitCheck.is_empty(raw_df):
                logger.info("No raw data from source — exiting early")
                self.job.commit()
                return

            # 3. Add lineage (NO business transforms in Bronze)
            bronze_df = self._add_audit_columns(raw_df)

            # 4. Optional raw DQ (warn+skip)
            ruleset = self._get_dq_ruleset_name()
            if ruleset:
                self.dq_manager.validate(bronze_df, ruleset)

            # 5. Write (append-only)
            self._write_output(bronze_df)

            self.job.commit()
            logger.info("Bronze job completed successfully")
        except Exception as e:
            logger.error(f"Bronze job failed: {e}", exc_info=True)
            raise


# ============================================================================
# EXAMPLE IMPLEMENTATION (delete and replace with your logic)
# ============================================================================
class BronzeSalesJob(BaseBronzeJob):
    """Example: land raw sales files into Bronze."""

    def _define_sources(self):
        # CHANGE_ME — point to your raw source
        return [
            {"type": "s3", "path": self.args.get("SOURCE_PATH", "s3://CHANGE_ME/raw/sales/"),
             "format": "parquet"},
        ]

    def _get_dq_ruleset_name(self):
        # Optional — return None to skip raw DQ
        return None


# ============================================================================
# ENTRY POINT
# ============================================================================
if __name__ == "__main__":
    BronzeSalesJob().run()
