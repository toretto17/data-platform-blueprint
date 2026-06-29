"""
================================================================================
CLASSIFICATION PROJECT TEMPLATE — [Databricks]
================================================================================
Purpose: Binary/multi-class classification with Feature Store integration,
         hyperparameter tuning (Optuna), cross-validation, and MLflow tracking.

Pattern:
    1. Load features from Feature Store (fe.create_training_set)
    2. Train/test split (stratified for imbalanced classes)
    3. Hyperparameter tuning with Optuna (recommended over deprecated Hyperopt)
    4. Cross-validate best params, evaluate on holdout
    5. Log best model + register in UC

Best practices:
    - Stratified split for imbalanced data
    - Optuna for HPO (Databricks recommendation as of 2025; SparkTrials deprecated)
    - Track precision/recall/F1/AUC — not just accuracy
    - SHAP for explainability (log as artifact)

Customize: FEATURE_TABLE, LABEL, algorithm, search_space, cv_folds.
AWS twin: aws/src/data_science/classification/classification.py
Version : 2026-06-29
================================================================================
"""
import logging
from typing import Optional, Dict

import mlflow
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

logger = logging.getLogger("classification_databricks")
spark = SparkSession.builder.getOrCreate()


class ClassificationProject:
    # ---- CHANGE_ME ----
    FEATURE_TABLE: str = "main.features.customer_features"
    LABEL: str = "churn"
    PRIMARY_KEYS: list = ["customer_id"]
    EXPERIMENT: str = "/Shared/experiments/classification"
    MODEL_NAME: str = "main.ml.churn_classifier"
    N_TRIALS: int = 50                                    # Optuna trials
    CV_FOLDS: int = 5

    def __init__(self, cfg: Optional[dict] = None):
        if cfg:
            for k, v in cfg.items():
                if hasattr(self, k.upper()):
                    setattr(self, k.upper(), v)
        mlflow.set_experiment(self.EXPERIMENT)

    # ---- 1. load data via Feature Store ----
    def load_data(self):
        from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
        fe = FeatureEngineeringClient()
        label_df = spark.table("main.gold.labels")       # CHANGE_ME: must have PK + label
        ts = fe.create_training_set(
            df=label_df,
            feature_lookups=[FeatureLookup(table_name=self.FEATURE_TABLE,
                                           feature_names=None, lookup_key=self.PRIMARY_KEYS)],
            label=self.LABEL, exclude_columns=self.PRIMARY_KEYS)
        df = ts.load_df().toPandas()
        X = df.drop(columns=[self.LABEL])
        y = df[self.LABEL]
        return X, y, ts

    # ---- 2. stratified split ----
    def split(self, X, y, test_size: float = 0.2):
        from sklearn.model_selection import train_test_split
        return train_test_split(X, y, test_size=test_size, stratify=y, random_state=42)

    # ---- 3. HPO with Optuna (Databricks-recommended, replaces Hyperopt) ----
    def tune(self, X_train, y_train) -> Dict:
        """Run Optuna HPO. Returns best params."""
        import optuna
        from sklearn.ensemble import GradientBoostingClassifier
        from sklearn.model_selection import cross_val_score

        def objective(trial):
            params = {
                "n_estimators": trial.suggest_int("n_estimators", 50, 300),
                "max_depth": trial.suggest_int("max_depth", 3, 10),
                "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
                "subsample": trial.suggest_float("subsample", 0.6, 1.0),
                "min_samples_leaf": trial.suggest_int("min_samples_leaf", 2, 20),
            }
            clf = GradientBoostingClassifier(**params, random_state=42)
            scores = cross_val_score(clf, X_train, y_train, cv=self.CV_FOLDS, scoring="f1_weighted")
            return scores.mean()

        study = optuna.create_study(direction="maximize")
        study.optimize(objective, n_trials=self.N_TRIALS, show_progress_bar=True)
        logger.info(f"best trial: {study.best_trial.value:.4f} params={study.best_params}")
        return study.best_params

    # ---- 4. train final model + evaluate ----
    def train_and_evaluate(self, best_params, X_train, y_train, X_test, y_test):
        from sklearn.ensemble import GradientBoostingClassifier
        from sklearn.metrics import classification_report, f1_score, roc_auc_score

        model = GradientBoostingClassifier(**best_params, random_state=42)
        model.fit(X_train, y_train)
        preds = model.predict(X_test)
        proba = model.predict_proba(X_test)[:, 1] if len(np.unique(y_test)) == 2 else None

        metrics = {
            "f1": f1_score(y_test, preds, average="weighted"),
            "roc_auc": roc_auc_score(y_test, proba) if proba is not None else 0,
        }
        report = classification_report(y_test, preds)
        logger.info(f"evaluation: {metrics}\n{report}")
        return model, metrics, report

    # ---- 5. run ----
    def run(self):
        X, y, ts = self.load_data()
        X_train, X_test, y_train, y_test = self.split(X, y)

        with mlflow.start_run(run_name="classification"):
            best_params = self.tune(X_train, y_train)
            model, metrics, report = self.train_and_evaluate(best_params, X_train, y_train, X_test, y_test)

            # Log everything
            mlflow.log_params(best_params)
            for k, v in metrics.items():
                mlflow.log_metric(k, v)
            mlflow.log_text(report, "classification_report.txt")

            # Register via Feature Engineering (enables score_batch)
            from databricks.feature_engineering import FeatureEngineeringClient
            fe = FeatureEngineeringClient()
            fe.log_model(model=model, artifact_path="model", flavor=mlflow.sklearn,
                         training_set=ts, registered_model_name=self.MODEL_NAME)
            logger.info(f"model registered: {self.MODEL_NAME}")


if __name__ == "__main__":
    ClassificationProject({
        "feature_table": "main.features.customer_features",   # CHANGE_ME
        "label": "churn", "primary_keys": ["customer_id"],
    }).run()
