"""
================================================================================
BATCH + REAL-TIME INFERENCE — [Databricks]
================================================================================
Purpose: Score data using a registered model.
    • Batch: fe.score_batch (auto feature lookup from FS) or plain model.predict
    • Real-time: create a Model Serving endpoint (REST) via Databricks SDK

Verified API (docs.databricks.com):
    # Batch (Feature Store auto-lookup):
    predictions = fe.score_batch(model_uri="models:/catalog.schema.model/1", df=batch_df)

    # Realtime (Model Serving):
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    w.serving_endpoints.create(name=..., config=EndpointCoreConfigInput(
        served_entities=[ServedEntityInput(entity_name="catalog.schema.model", entity_version="1",
                                            workload_size="Small", scale_to_zero_enabled=True)]))

Cost-effective options:
    - Batch: run on standard cluster (no Photon needed); score_batch uses Spark parallelism.
    - Realtime: scale_to_zero_enabled=True (pay only when requests come in).
    - GPU: workload_type="GPU_SMALL" for DL models (optional).

Customize: MODEL_URI, batch_df source, endpoint name/config.
AWS twin: aws/src/mlops/inference/inference.py (SageMaker Batch Transform + Endpoint).
Version : 2026-06-29
================================================================================
"""
import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger("inference_databricks")
spark = SparkSession.builder.getOrCreate()


class BatchInferenceDatabricks:
    """Score a batch using a model registered in Unity Catalog.
    Uses fe.score_batch for automatic feature lookup (if model was logged with fe.log_model)."""

    MODEL_URI: str = "models:/main.ml.churn_model/latest"  # CHANGE_ME
    OUTPUT_TABLE: str = "main.gold.predictions"            # CHANGE_ME (write predictions)

    def __init__(self, cfg: Optional[dict] = None):
        if cfg:
            for k, v in cfg.items():
                if hasattr(self, k.upper()):
                    setattr(self, k.upper(), v)
        self._fe = None

    @property
    def fe(self):
        if self._fe is None:
            from databricks.feature_engineering import FeatureEngineeringClient
            self._fe = FeatureEngineeringClient()
        return self._fe

    def score(self, batch_df: DataFrame) -> DataFrame:
        """Score batch_df (must contain primary keys of the feature table).
        Feature values are auto-looked up from the feature table."""
        predictions = self.fe.score_batch(model_uri=self.MODEL_URI, df=batch_df)
        logger.info(f"batch scored: {predictions.count()} predictions")
        return predictions

    def score_and_write(self, batch_df: DataFrame, mode: str = "append"):
        """Score + write predictions to a Delta table."""
        preds = self.score(batch_df)
        preds.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(self.OUTPUT_TABLE)
        logger.info(f"predictions written → {self.OUTPUT_TABLE}")
        return preds


class RealtimeEndpointDatabricks:
    """Create / update a Databricks Model Serving endpoint (REST API).
    scale_to_zero_enabled=True makes it cost-effective (no idle cost)."""

    ENDPOINT_NAME: str = "churn-model-endpoint"            # CHANGE_ME
    MODEL_NAME: str = "main.ml.churn_model"                # CHANGE_ME (UC model)
    MODEL_VERSION: str = "latest"                          # or specific version number
    WORKLOAD_SIZE: str = "Small"                           # Small | Medium | Large
    SCALE_TO_ZERO: bool = True                             # cost-effective default
    WORKLOAD_TYPE: Optional[str] = None                    # None = CPU; "GPU_SMALL" for DL

    def __init__(self, cfg: Optional[dict] = None):
        if cfg:
            for k, v in cfg.items():
                if hasattr(self, k.upper()):
                    setattr(self, k.upper(), v)

    def create_or_update(self):
        """Create or update the serving endpoint (idempotent)."""
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.serving import (
            EndpointCoreConfigInput, ServedEntityInput,
        )

        w = WorkspaceClient()
        entity = ServedEntityInput(
            entity_name=self.MODEL_NAME,
            entity_version=self.MODEL_VERSION if self.MODEL_VERSION != "latest" else None,
            workload_size=self.WORKLOAD_SIZE,
            scale_to_zero_enabled=self.SCALE_TO_ZERO,
            workload_type=self.WORKLOAD_TYPE,
        )
        config = EndpointCoreConfigInput(served_entities=[entity])

        # Check if endpoint exists
        try:
            existing = w.serving_endpoints.get(self.ENDPOINT_NAME)
            w.serving_endpoints.update_config(self.ENDPOINT_NAME, served_entities=[entity])
            logger.info(f"updated endpoint: {self.ENDPOINT_NAME}")
        except Exception:
            w.serving_endpoints.create(name=self.ENDPOINT_NAME, config=config)
            logger.info(f"created endpoint: {self.ENDPOINT_NAME}")

    def query(self, payload: dict) -> dict:
        """Send a scoring request to the endpoint (for testing)."""
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        resp = w.serving_endpoints.query(self.ENDPOINT_NAME, dataframe_records=[payload])
        return resp.as_dict()


if __name__ == "__main__":
    # Batch example
    batch_df = spark.table("main.gold.customers_to_score")  # CHANGE_ME: must have PK columns
    scorer = BatchInferenceDatabricks({"model_uri": "models:/main.ml.churn_model/latest",
                                        "output_table": "main.gold.churn_predictions"})
    scorer.score_and_write(batch_df)

    # Realtime example (create/update endpoint)
    # ep = RealtimeEndpointDatabricks({"endpoint_name": "churn-v1", "model_name": "main.ml.churn_model"})
    # ep.create_or_update()
