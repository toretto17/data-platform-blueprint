"""
Databricks Inference — Quick Start Template
=============================================
Full implementation: databricks/src/mlops/inference/inference.py

Batch (fe.score_batch — auto feature lookup) + Realtime (Model Serving, scale-to-zero).
"""
# Quick usage:
#   from databricks.src.mlops.inference.inference import BatchInferenceDatabricks, RealtimeEndpointDatabricks
#
#   # Batch (auto-fetches features from Feature Store):
#   scorer = BatchInferenceDatabricks({"model_uri": "models:/main.ml.CHANGE_ME/latest"})
#   predictions = scorer.score(batch_df)
#
#   # Realtime (scale-to-zero = no idle cost):
#   ep = RealtimeEndpointDatabricks({"endpoint_name": "CHANGE_ME", "model_name": "main.ml.CHANGE_ME"})
#   ep.create_or_update()

MODEL_URI = "models:/main.ml.CHANGE_ME/latest"   # CHANGE_ME
ENDPOINT_NAME = "CHANGE_ME"
# See databricks/src/mlops/inference/inference.py for full implementation.
