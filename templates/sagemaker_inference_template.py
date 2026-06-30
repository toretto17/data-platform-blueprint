"""
SageMaker Inference — Quick Start Template
============================================
Full implementation: aws/src/mlops/inference/inference.py

Batch Transform + Realtime Endpoint (Serverless).
"""
# Quick usage:
#   from aws.src.mlops.inference.inference import BatchTransformAWS, RealtimeEndpointAWS
#
#   # Batch:
#   bt = BatchTransformAWS({"model_package_group": "CHANGE_ME", "input_s3": "s3://...", "output_s3": "s3://..."})
#   bt.submit()
#
#   # Realtime (Serverless — pay per invocation):
#   ep = RealtimeEndpointAWS({"endpoint_name": "CHANGE_ME", "serverless": True})
#   ep.create_or_update()

MODEL_PACKAGE_GROUP = "CHANGE_ME"
INSTANCE_TYPE = "ml.m5.xlarge"  # CHANGE_ME
# See aws/src/mlops/inference/inference.py for full implementation.
