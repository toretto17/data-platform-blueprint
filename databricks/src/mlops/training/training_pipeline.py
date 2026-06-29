"""
================================================================================
MODEL TRAINING — [Databricks / MLflow / Unity Catalog]
================================================================================
Purpose: Template for training an ML model on features from the Feature Store,
         logging it with MLflow (auto lineage), and optionally registering in UC.

Verified pattern (docs.databricks.com):
    1. Build training set from Feature Store (fe.create_training_set + FeatureLookup)
    2. Train model (any sklearn/xgboost/lightgbm/spark/custom)
    3. Log with fe.log_model (packages feature lookup info — enables score_batch)
    4. Register in Unity Catalog (mlflow.register_model)
    5. Metrics + params tracked automatically in MLflow Experiment

Key decisions (from production + docs):
    - Use fe.log_model (NOT plain mlflow.sklearn.log_model) so score_batch works
    - Register in UC (3-level: catalog.schema.model_name), not workspace registry
    - Autolog for convenience BUT explicit metric logging for custom metrics
    - No Photon dependency; works on standard clusters

Customize (CHANGE_ME):
    - _get_training_data(): your label df + feature lookups
    - _train_model(): your algorithm
    - MODEL_NAME, EXPERIMENT_NAME

Platform notes: DBR 13.3 LTS ML+ (pre-installed MLflow 2.x + databricks-feature-engineering).
AWS twin: aws/src/mlops/training/training_pipeline.py (SageMaker Pipelines @step).
Version : 2026-06-29
================================================================================
"""
import logging
from typing import Optional

import mlflow
from pyspark.sql import SparkSession

logger = logging.getLogger("training_databricks")
spark = SparkSession.builder.getOrCreate()


class ModelTrainerDatabricks:
    # ---- CHANGE_ME ----
    EXPERIMENT_NAME: str = "/Shared/experiments/my_model"      # MLflow experiment
    MODEL_NAME: str = "main.ml.my_model"                       # UC 3-level model name
    FEATURE_TABLE: str = "main.features.customer_features"     # feature table for lookups
    PRIMARY_KEYS: list = ["customer_id"]
    LABEL: str = "churn"
    FEATURE_NAMES: Optional[list] = None                       # None = all features

    def __init__(self, cfg: Optional[dict] = None):
        if cfg:
            for k, v in cfg.items():
                if hasattr(self, k.upper()):
                    setattr(self, k.upper(), v)
        mlflow.set_experiment(self.EXPERIMENT_NAME)
        self._fe = None

    @property
    def fe(self):
        if self._fe is None:
            from databricks.feature_engineering import FeatureEngineeringClient
            self._fe = FeatureEngineeringClient()
        return self._fe

    # ---- 1. training data from Feature Store ----
    def _get_training_data(self):
        """Build training set with auto PIT join. Returns (training_set, X, y)."""
        from databricks.feature_engineering import FeatureLookup

        # Label DataFrame (must contain lookup_key + label; can contain extra cols)
        label_df = spark.table("main.gold.labels")             # CHANGE_ME

        feature_lookups = [
            FeatureLookup(
                table_name=self.FEATURE_TABLE,
                feature_names=self.FEATURE_NAMES,              # None = all non-PK features
                lookup_key=self.PRIMARY_KEYS,
            ),
        ]

        training_set = self.fe.create_training_set(
            df=label_df,
            feature_lookups=feature_lookups,
            label=self.LABEL,
            exclude_columns=self.PRIMARY_KEYS,
        )
        df = training_set.load_df().toPandas()
        X = df.drop(columns=[self.LABEL])
        y = df[self.LABEL]
        return training_set, X, y

    # ---- 2. train model (CHANGE_ME: your algorithm) ----
    def _train_model(self, X, y):
        """Train and return a fitted model. Override with your algorithm."""
        from sklearn.ensemble import GradientBoostingClassifier

        model = GradientBoostingClassifier(
            n_estimators=100, max_depth=5, learning_rate=0.1, random_state=42
        )
        model.fit(X, y)
        return model

    # ---- 3. evaluate ----
    def _evaluate(self, model, X, y) -> dict:
        """Return metrics dict. CHANGE_ME with your eval logic."""
        from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
        preds = model.predict(X)
        proba = model.predict_proba(X)[:, 1] if hasattr(model, "predict_proba") else preds
        return {
            "accuracy": accuracy_score(y, preds),
            "f1": f1_score(y, preds, average="weighted"),
            "roc_auc": roc_auc_score(y, proba),
        }

    # ---- 4. log + register ----
    def _log_and_register(self, model, training_set, metrics: dict):
        """Log model with fe.log_model (enables score_batch) + register in UC."""
        with mlflow.start_run() as run:
            # Log metrics
            for k, v in metrics.items():
                mlflow.log_metric(k, v)

            # Log model with feature-store packaging (enables auto feature lookup at inference)
            self.fe.log_model(
                model=model,
                artifact_path="model",
                flavor=mlflow.sklearn,                     # CHANGE_ME per framework
                training_set=training_set,
                registered_model_name=self.MODEL_NAME,     # auto-registers in UC
            )
            logger.info(f"model logged + registered: {self.MODEL_NAME} (run={run.info.run_id})")
            logger.info(f"metrics: {metrics}")
        return run.info.run_id

    # ---- run ----
    def run(self):
        try:
            training_set, X, y = self._get_training_data()
            model = self._train_model(X, y)
            metrics = self._evaluate(model, X, y)

            # Gate: only register if metrics pass threshold (CHANGE_ME)
            if metrics.get("f1", 0) < 0.5:
                logger.warning(f"model below threshold (f1={metrics['f1']:.3f}) — NOT registering")
                mlflow.start_run()
                for k, v in metrics.items():
                    mlflow.log_metric(k, v)
                mlflow.log_param("registered", False)
                mlflow.end_run()
                return

            self._log_and_register(model, training_set, metrics)
            logger.info("training pipeline complete")
        except Exception as e:
            logger.error(f"training failed: {e}")
            raise


if __name__ == "__main__":
    ModelTrainerDatabricks({
        "experiment_name": "/Shared/experiments/churn_model",  # CHANGE_ME
        "model_name": "main.ml.churn_model",                   # CHANGE_ME
        "feature_table": "main.features.customer_features",
        "primary_keys": ["customer_id"],
        "label": "churn",
    }).run()
