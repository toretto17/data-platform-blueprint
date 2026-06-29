"""
================================================================================
EXPERIMENT TRACKING + HPO UTILITIES — [Databricks]
================================================================================
Purpose: Reusable helpers for experiment management and hyperparameter tuning.
         Wraps Optuna + MLflow so DS teams get consistent experiment tracking
         without boilerplate.

Contents:
    - ExperimentManager: create/set/get experiments, compare runs, get best run
    - OptunaMLflowCallback: auto-logs every Optuna trial to MLflow
    - quick_tune(): one-liner HPO with auto-logging

Verified APIs (Databricks docs 2025):
    - Optuna (recommended over deprecated Hyperopt SparkTrials)
    - MLflow 2.x (experiment tracking, run comparison, artifact logging)

Best practices:
    - One experiment per model/project (not per notebook)
    - Tag runs with team/environment/version for filtering
    - Use nested runs for HPO trials (keeps parent run clean)
    - Log artifacts (SHAP, confusion matrix, leaderboard) not just metrics

Customize: EXPERIMENT_NAME, objective function, search_space.
AWS twin: aws/src/data_science/experimentation/experimentation.py
Version : 2026-06-29
================================================================================
"""
import logging
from typing import Callable, Dict, Optional

import mlflow
from mlflow.tracking import MlflowClient

logger = logging.getLogger("experimentation_databricks")


class ExperimentManager:
    """Manage MLflow experiments: create, query, compare."""

    def __init__(self, experiment_name: str):
        self.experiment_name = experiment_name
        mlflow.set_experiment(experiment_name)
        self.client = MlflowClient()

    def get_best_run(self, metric: str = "f1", order: str = "DESC") -> Optional[dict]:
        """Get the run with the best metric value in this experiment."""
        exp = self.client.get_experiment_by_name(self.experiment_name)
        if not exp:
            return None
        runs = self.client.search_runs(
            experiment_ids=[exp.experiment_id],
            order_by=[f"metrics.{metric} {order}"],
            max_results=1)
        if not runs:
            return None
        r = runs[0]
        return {"run_id": r.info.run_id, "metrics": r.data.metrics, "params": r.data.params}

    def compare_runs(self, metric: str = "f1", top_n: int = 5) -> list:
        """Return top N runs sorted by metric."""
        exp = self.client.get_experiment_by_name(self.experiment_name)
        if not exp:
            return []
        runs = self.client.search_runs(
            experiment_ids=[exp.experiment_id],
            order_by=[f"metrics.{metric} DESC"],
            max_results=top_n)
        return [{"run_id": r.info.run_id, metric: r.data.metrics.get(metric),
                 "params": r.data.params} for r in runs]


class OptunaMLflowCallback:
    """Optuna callback that logs each trial as a nested MLflow run.
    Usage: study.optimize(objective, n_trials=50, callbacks=[OptunaMLflowCallback()])"""

    def __call__(self, study, trial):
        with mlflow.start_run(run_name=f"trial_{trial.number}", nested=True):
            mlflow.log_params(trial.params)
            mlflow.log_metric("objective_value", trial.value)
            mlflow.log_metric("trial_number", trial.number)


def quick_tune(objective: Callable, n_trials: int = 50, direction: str = "maximize",
               experiment_name: Optional[str] = None) -> Dict:
    """One-liner HPO: run Optuna with auto-MLflow logging. Returns best_params.

    Usage:
        def objective(trial):
            lr = trial.suggest_float("lr", 0.001, 0.1, log=True)
            ...train model...
            return f1_score

        best = quick_tune(objective, n_trials=50, experiment_name="/Shared/experiments/my_model")
    """
    import optuna
    if experiment_name:
        mlflow.set_experiment(experiment_name)

    with mlflow.start_run(run_name="hpo_session"):
        study = optuna.create_study(direction=direction)
        study.optimize(objective, n_trials=n_trials, callbacks=[OptunaMLflowCallback()])
        mlflow.log_params(study.best_params)
        mlflow.log_metric("best_objective", study.best_value)
        logger.info(f"HPO complete: best={study.best_value:.4f} params={study.best_params}")
    return study.best_params
