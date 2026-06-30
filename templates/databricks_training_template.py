"""
Databricks Training — Quick Start Template
============================================
Full implementation: databricks/src/mlops/training/training_pipeline.py

Feature Store → Train → Evaluate (gate) → Register in UC.
"""
# Quick usage:
#   from databricks.src.mlops.training.training_pipeline import ModelTrainerDatabricks
#   trainer = ModelTrainerDatabricks(cfg)
#   trainer.run()
#
# Or configure directly:

EXPERIMENT_NAME = "/Shared/experiments/CHANGE_ME"
MODEL_NAME = "main.ml.CHANGE_ME"            # UC 3-level name
FEATURE_TABLE = "main.features.CHANGE_ME"
PRIMARY_KEYS = ["CHANGE_ME"]
LABEL = "CHANGE_ME"

# See databricks/src/mlops/training/training_pipeline.py for the full class.
