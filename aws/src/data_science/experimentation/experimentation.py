"""
================================================================================
EXPERIMENT TRACKING + HPO UTILITIES — [AWS SageMaker]
================================================================================
Purpose: Same helpers as Databricks twin but using SageMaker Experiments or
         local MLflow. Optuna for HPO (same as Databricks — portable).

AWS-specific: SageMaker Experiments auto-tracks @step runs. For Processing
jobs outside Pipelines, use local MLflow or the SM Experiments SDK directly.

Contents: ExperimentManager, OptunaCallback, quick_tune() — same API as DBX twin.
Databricks twin: databricks/src/data_science/experimentation/experimentation.py
Version : 2026-06-29
================================================================================
"""
import logging
from typing import Callable, Dict, Optional
import json

logger = logging.getLogger("experimentation_aws")


class ExperimentManager:
    """Manage experiments via local JSON tracking (portable) or MLflow if available."""

    def __init__(self, experiment_name: str, output_dir: str = "/opt/ml/processing/output"):
        self.experiment_name = experiment_name
        self.output_dir = output_dir
        self._trials = []

    def log_trial(self, params: dict, metrics: dict):
        self._trials.append({"params": params, "metrics": metrics})

    def get_best_trial(self, metric: str = "f1") -> Optional[dict]:
        if not self._trials:
            return None
        return max(self._trials, key=lambda t: t["metrics"].get(metric, 0))

    def save(self):
        import os
        os.makedirs(self.output_dir, exist_ok=True)
        with open(f"{self.output_dir}/experiment_{self.experiment_name}.json", "w") as f:
            json.dump({"experiment": self.experiment_name, "trials": self._trials,
                       "best": self.get_best_trial()}, f, indent=2, default=str)


def quick_tune(objective: Callable, n_trials: int = 50, direction: str = "maximize") -> Dict:
    """One-liner Optuna HPO (same API as Databricks twin — portable)."""
    import optuna
    study = optuna.create_study(direction=direction)
    study.optimize(objective, n_trials=n_trials)
    logger.info(f"HPO: best={study.best_value:.4f} params={study.best_params}")
    return study.best_params
