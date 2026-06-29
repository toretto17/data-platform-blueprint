"""
================================================================================
MODEL REGISTRY + PROMOTION — [Databricks / MLflow / Unity Catalog]
================================================================================
Purpose: Register a model version in UC, promote between stages (None → Champion),
         and retrieve the latest production model URI.

Verified pattern (docs.databricks.com):
    - Models registered in UC: mlflow.register_model("runs:/<run_id>/model", "catalog.schema.model")
    - Versions tracked automatically per registration call
    - Stage transitions via aliases: client.set_registered_model_alias(name, "Champion", version)
    - Resolve: client.get_model_version_by_alias(name, "Champion")
    - No legacy "Staging/Production" stages — UC uses ALIASES ("Champion", "Challenger", etc.)

    Note: fe.log_model(..., registered_model_name=...) also registers the model.
    This module is for when you need to promote / compare / retrieve programmatically.

Customize: MODEL_NAME, ALIAS_MAP, evaluation gates.
AWS twin: aws/src/mlops/registry/registry.py (SageMaker Model Registry — PackageGroup + Approve).
Version : 2026-06-29
================================================================================
"""
import logging
from typing import Optional

import mlflow
from mlflow.tracking import MlflowClient

logger = logging.getLogger("registry_databricks")


class ModelRegistryDatabricks:
    MODEL_NAME: str = "main.ml.churn_model"               # CHANGE_ME (UC 3-level)

    def __init__(self, cfg: Optional[dict] = None):
        if cfg:
            for k, v in cfg.items():
                if hasattr(self, k.upper()):
                    setattr(self, k.upper(), v)
        self.client = MlflowClient()

    def register(self, run_id: str, artifact_path: str = "model") -> str:
        """Register a logged model (by run_id) into UC. Returns the model version string."""
        uri = f"runs:/{run_id}/{artifact_path}"
        mv = mlflow.register_model(uri, self.MODEL_NAME)
        logger.info(f"registered {self.MODEL_NAME} version={mv.version}")
        return mv.version

    def promote_to_champion(self, version: str):
        """Set the 'Champion' alias on a version (makes it the serving default)."""
        self.client.set_registered_model_alias(self.MODEL_NAME, "Champion", int(version))
        logger.info(f"promoted {self.MODEL_NAME} version={version} → alias=Champion")

    def set_alias(self, alias: str, version: str):
        """Set an arbitrary alias (e.g. Challenger, Canary)."""
        self.client.set_registered_model_alias(self.MODEL_NAME, alias, int(version))
        logger.info(f"alias {alias} → {self.MODEL_NAME} version={version}")

    def get_champion_version(self) -> Optional[str]:
        """Get the version number of the current Champion. None if no alias set."""
        try:
            mv = self.client.get_model_version_by_alias(self.MODEL_NAME, "Champion")
            return str(mv.version)
        except Exception:
            return None

    def get_champion_uri(self) -> str:
        """Return the model URI of the current Champion (for inference)."""
        return f"models:/{self.MODEL_NAME}@Champion"

    def get_latest_version(self) -> str:
        """Return the latest version number (regardless of alias)."""
        versions = self.client.search_model_versions(f"name='{self.MODEL_NAME}'",
                                                      order_by=["version_number DESC"],
                                                      max_results=1)
        return str(versions[0].version) if versions else "0"

    def compare_champion_vs_challenger(self, challenger_version: str,
                                        metric: str = "eval_f1") -> dict:
        """Compare metrics between Champion and a Challenger version."""
        champion_v = self.get_champion_version()
        if not champion_v:
            return {"champion": None, "challenger": challenger_version, "winner": "challenger"}

        def _get_metric(version):
            mv = self.client.get_model_version(self.MODEL_NAME, version)
            run = self.client.get_run(mv.run_id)
            return run.data.metrics.get(metric, 0)

        c_score = _get_metric(champion_v)
        ch_score = _get_metric(challenger_version)
        winner = "challenger" if ch_score > c_score else "champion"
        logger.info(f"compare: champion(v{champion_v})={c_score:.4f} vs "
                    f"challenger(v{challenger_version})={ch_score:.4f} → {winner}")
        return {"champion_score": c_score, "challenger_score": ch_score,
                "champion_version": champion_v, "challenger_version": challenger_version,
                "winner": winner}


if __name__ == "__main__":
    reg = ModelRegistryDatabricks({"model_name": "main.ml.churn_model"})  # CHANGE_ME
    # Register from a run
    # version = reg.register(run_id="abc123")
    # Promote
    # reg.promote_to_champion(version)
    # Get serving URI
    print(f"Champion URI: {reg.get_champion_uri()}")
