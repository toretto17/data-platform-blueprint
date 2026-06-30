"""
SageMaker Training — Quick Start Template
==========================================
Full implementation: aws/src/mlops/training/training_pipeline.py

Copy this file, fill CHANGE_ME, and you have a working training job.
"""
# Quick usage:
#   from aws.src.mlops.training.training_pipeline import ModelTrainerAWS
#   trainer = ModelTrainerAWS()
#   trainer.run()
#
# Or use this starter directly:

EXPERIMENT_NAME = "CHANGE_ME"
MODEL_NAME = "CHANGE_ME"
FEATURE_TABLE = "CHANGE_ME"
LABEL = "CHANGE_ME"
THRESHOLDS = {"f1": 0.6, "roc_auc": 0.7}  # CHANGE_ME: your eval gates

# See aws/src/mlops/training/training_pipeline.py for the full Base class.
