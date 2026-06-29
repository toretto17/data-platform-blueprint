"""
================================================================================
FEATURE TABLE — create / read / describe  [Databricks Feature Engineering in UC]
================================================================================
Purpose: Manage a Unity Catalog feature table. In Databricks, ANY Delta table with
         a primary key constraint is a feature table — no special "Feature Group"
         entity needed. The `FeatureEngineeringClient` wraps creation + writes +
         training-set building with lineage tracking.

Verified API (docs.databricks.com, Jun 2026):
    from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
    fe = FeatureEngineeringClient()

    fe.create_table(
        name="catalog.schema.table",
        primary_keys=["pk_col"],             # or list of cols
        schema=spark_df.schema,              # inferred from df if using df= arg
        timeseries_columns="ts_col",         # optional — enables PIT lookups
        description="..."
    )

    fe.write_table(name=..., df=..., mode="merge")   # "merge" | "overwrite"
    fe.read_table(name=...)                           # returns DataFrame

    training_set = fe.create_training_set(
        df=labeled_df,
        feature_lookups=[FeatureLookup(table_name=..., feature_names=[...], lookup_key=...)],
        label="target_col",
        exclude_columns=["pk_col"]
    )
    training_df = training_set.load_df()

    fe.log_model(model=..., artifact_path=..., flavor=mlflow.sklearn, training_set=training_set)
    predictions = fe.score_batch(model_uri=..., df=batch_df)

Requirements:
    - databricks-feature-engineering package (pre-installed on DBR 13.3 LTS ML+;
      install manually on non-ML runtimes: %pip install databricks-feature-engineering)
    - Unity Catalog enabled workspace
    - Privilege: USE CATALOG, USE SCHEMA, CREATE TABLE on the target schema

Customize (CHANGE_ME):
    - CATALOG, SCHEMA, TABLE, PRIMARY_KEYS, TIMESERIES_COL
    - The feature computation function (returns a Spark DataFrame)

Platform notes:
    - DBR 13.2+ (feature tables); 13.3 LTS ML+ (pre-installed client).
    - AWS twin: aws/src/feature_store/creation/feature_group.py (SageMaker FeatureGroup).
    - Cost-effective note: No Photon dependency. Works on standard clusters.
      For large writes, Photon ACCELERATES but is NOT required.
Version : 2026-06-29
================================================================================
"""
import logging
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

logger = logging.getLogger("feature_group_databricks")
spark = SparkSession.builder.getOrCreate()


class FeatureTableManager:
    """Manage a Databricks Unity Catalog feature table.
    Wraps FeatureEngineeringClient for create / write / read / training-set."""

    def __init__(self, catalog: str, schema: str, table: str,
                 primary_keys: List[str], timeseries_col: Optional[str] = None,
                 description: str = ""):
        self.full_name = f"{catalog}.{schema}.{table}"     # UC 3-level name
        self.primary_keys = primary_keys
        self.timeseries_col = timeseries_col
        self.description = description
        self._fe = None  # lazy init (avoids import failure on non-ML runtimes)

    @property
    def fe(self):
        if self._fe is None:
            from databricks.feature_engineering import FeatureEngineeringClient
            self._fe = FeatureEngineeringClient()
        return self._fe

    # ==================================================================
    # CREATE (idempotent — skips if table already exists)
    # ==================================================================
    def create_table(self, spark_schema: Optional[StructType] = None,
                     df: Optional[DataFrame] = None):
        """Create the feature table in UC. Provide either `spark_schema` OR `df` (not both).
        If `df` is passed, the data is also written on creation.
        Idempotent: if the table already exists, logs a warning and returns."""
        if spark.catalog.tableExists(self.full_name):
            logger.info(f"feature table already exists: {self.full_name} — skipping create")
            return

        kwargs = dict(
            name=self.full_name,
            primary_keys=self.primary_keys,
            description=self.description,
        )
        if self.timeseries_col:
            kwargs["timeseries_columns"] = self.timeseries_col

        if df is not None:
            kwargs["df"] = df   # schema inferred + data written in one call
        elif spark_schema is not None:
            kwargs["schema"] = spark_schema
        else:
            raise ValueError("provide either spark_schema or df to create_table")

        self.fe.create_table(**kwargs)
        logger.info(f"created feature table: {self.full_name} (PK={self.primary_keys}, "
                    f"ts={'none' if not self.timeseries_col else self.timeseries_col})")

    # ==================================================================
    # CREATE via SQL (alternative — no client needed, works everywhere)
    # ==================================================================
    def create_table_sql(self, columns_ddl: str):
        """Create using pure SQL (no client dependency). `columns_ddl` is the full column
        definition including the PK constraint. Example:
            create_table_sql("id INT NOT NULL, ts TIMESTAMP NOT NULL, feat1 DOUBLE, "
                             "CONSTRAINT pk PRIMARY KEY (id, ts TIMESERIES)")
        """
        spark.sql(f"CREATE TABLE IF NOT EXISTS {self.full_name} ({columns_ddl})")
        logger.info(f"created feature table via SQL: {self.full_name}")

    # ==================================================================
    # WRITE (batch or streaming)
    # ==================================================================
    def write(self, df: DataFrame, mode: str = "merge"):
        """Write features. mode='merge' updates existing rows by PK and inserts new.
        mode='overwrite' replaces the entire table.
        For streaming DataFrames, pass a streaming df — write_table returns a StreamingQuery."""
        self.fe.write_table(name=self.full_name, df=df, mode=mode)
        logger.info(f"write_table → {self.full_name} (mode={mode})")

    # ==================================================================
    # READ
    # ==================================================================
    def read(self) -> DataFrame:
        """Read the full feature table as a DataFrame."""
        return self.fe.read_table(name=self.full_name)

    # ==================================================================
    # TRAINING SET (Point-in-time join + lineage tracking)
    # ==================================================================
    def create_training_set(self, labeled_df: DataFrame, feature_names: Optional[List[str]] = None,
                            lookup_key: Optional[List[str]] = None, label: str = "label",
                            exclude_columns: Optional[List[str]] = None,
                            timestamp_lookup_key: Optional[str] = None):
        """Build a training set with automatic PIT join (if timeseries_col is set) +
        lineage tracking. Returns a TrainingSet (call .load_df() to get the DataFrame).

        Args:
            labeled_df: your labels + lookup key columns
            feature_names: which features to include (None = all non-PK features)
            lookup_key: column(s) in labeled_df that map to the table's PKs (defaults to primary_keys)
            label: target column name in labeled_df
            exclude_columns: columns to drop from the final training df
            timestamp_lookup_key: column in labeled_df for PIT join (if timeseries table)
        """
        from databricks.feature_engineering import FeatureLookup

        fl_kwargs = dict(
            table_name=self.full_name,
            feature_names=feature_names,
            lookup_key=lookup_key or self.primary_keys,
        )
        if timestamp_lookup_key:
            fl_kwargs["timestamp_lookup_key"] = timestamp_lookup_key

        feature_lookups = [FeatureLookup(**fl_kwargs)]

        ts = self.fe.create_training_set(
            df=labeled_df,
            feature_lookups=feature_lookups,
            label=label,
            exclude_columns=exclude_columns or [],
        )
        logger.info(f"training set created from {self.full_name} (label={label}, "
                    f"PIT={timestamp_lookup_key or 'none'})")
        return ts

    # ==================================================================
    # BATCH INFERENCE (score_batch — auto feature lookup)
    # ==================================================================
    def score_batch(self, model_uri: str, batch_df: DataFrame) -> DataFrame:
        """Score a batch using a model logged with fe.log_model (auto feature lookup)."""
        return self.fe.score_batch(model_uri=model_uri, df=batch_df)

    # ==================================================================
    # DESCRIBE
    # ==================================================================
    def describe(self) -> dict:
        """Return metadata about the feature table."""
        ft = self.fe.get_table(name=self.full_name)
        return {"name": ft.name, "primary_keys": ft.primary_keys,
                "features": [f.name for f in ft.features],
                "description": ft.description}

    # ==================================================================
    # DELETE
    # ==================================================================
    def drop(self):
        """Drop the feature table (and the underlying Delta table)."""
        self.fe.drop_table(name=self.full_name)
        logger.info(f"dropped feature table: {self.full_name}")


# ============================================================================
# EXAMPLE USAGE (delete + replace)
# ============================================================================
if __name__ == "__main__":
    # 1. Create feature table
    mgr = FeatureTableManager(
        catalog="main",                          # CHANGE_ME
        schema="features",                       # CHANGE_ME
        table="customer_features",               # CHANGE_ME
        primary_keys=["customer_id"],            # CHANGE_ME
        timeseries_col=None,                     # set e.g. "event_date" for PIT
        description="Customer purchase features",
    )

    # Compute features (your logic)
    customer_features_df = spark.table("main.silver.customers")  # CHANGE_ME

    # Create + write in one call
    mgr.create_table(df=customer_features_df)

    # 2. Incremental update (mode=merge upserts by PK)
    # new_features = compute_new_features(...)
    # mgr.write(new_features, mode="merge")

    # 3. Read features
    df = mgr.read()
    df.show(5)

    # 4. Build training set (auto PIT join if timeseries_col set)
    # labeled = spark.table("main.gold.labels")
    # ts = mgr.create_training_set(labeled, label="churn", exclude_columns=["customer_id"])
    # training_df = ts.load_df()

    # 5. Log model with lineage
    # import mlflow
    # fe = mgr.fe
    # fe.log_model(model=..., artifact_path="model", flavor=mlflow.sklearn, training_set=ts)

    # 6. Batch inference (auto feature lookup from the table)
    # predictions = mgr.score_batch(model_uri="models:/churn_model/1", batch_df=batch_df)
