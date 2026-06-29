"""
================================================================================
MODEL DEPLOYMENT + ROLLBACK — [Databricks Model Serving]
================================================================================
Purpose: Deploy a registered model to a serving endpoint and provide safe
         rollback. Uses UC aliases + traffic routing for canary/blue-green.

Pattern (verified docs.databricks.com):
    - Deploy = set endpoint to serve a new model version
    - Canary: serve multiple entities with traffic_percentage split
    - Rollback: update endpoint config to previous version (< 30s)
    - All via databricks.sdk WorkspaceClient

Key best practices:
    - scale_to_zero_enabled=True (cost-effective; no idle cost)
    - Use aliases (Champion/Challenger) for stable model references
    - Canary: split traffic (e.g. 90% Champion / 10% Challenger)
    - Rollback: update served_entities back to Champion-only

Customize: ENDPOINT_NAME, MODEL_NAME, versions/aliases, traffic_config.
AWS twin: aws/src/mlops/deployment/deployment.py
Version : 2026-06-29
================================================================================
"""
import logging
from typing import Optional

logger = logging.getLogger("deployment_databricks")


class ModelDeploymentDatabricks:
    # ---- CHANGE_ME ----
    ENDPOINT_NAME: str = "churn-model-endpoint"          # CHANGE_ME
    MODEL_NAME: str = "main.ml.churn_model"              # CHANGE_ME UC 3-level
    WORKLOAD_SIZE: str = "Small"                         # Small | Medium | Large
    SCALE_TO_ZERO: bool = True                           # cost-effective default

    def __init__(self, cfg: Optional[dict] = None):
        if cfg:
            for k, v in cfg.items():
                if hasattr(self, k.upper()):
                    setattr(self, k.upper(), v)
        self._w = None

    @property
    def w(self):
        if self._w is None:
            from databricks.sdk import WorkspaceClient
            self._w = WorkspaceClient()
        return self._w

    def _entity(self, version: Optional[str] = None, alias: Optional[str] = None, traffic: int = 100):
        from databricks.sdk.service.serving import ServedEntityInput
        kwargs = {
            "entity_name": self.MODEL_NAME,
            "workload_size": self.WORKLOAD_SIZE,
            "scale_to_zero_enabled": self.SCALE_TO_ZERO,
        }
        if version:
            kwargs["entity_version"] = version
        # Note: alias-based serving uses entity_name@alias in some SDK versions
        return ServedEntityInput(**kwargs), traffic

    # ---- Deploy full (100% to a specific version) ----
    def deploy(self, version: str):
        """Deploy a model version to the endpoint (100% traffic)."""
        from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput
        entity = ServedEntityInput(
            entity_name=self.MODEL_NAME, entity_version=version,
            workload_size=self.WORKLOAD_SIZE, scale_to_zero_enabled=self.SCALE_TO_ZERO)
        config = EndpointCoreConfigInput(served_entities=[entity])
        try:
            self.w.serving_endpoints.get(self.ENDPOINT_NAME)
            self.w.serving_endpoints.update_config(self.ENDPOINT_NAME, served_entities=[entity])
            logger.info(f"updated endpoint {self.ENDPOINT_NAME} → v{version} (100%)")
        except Exception:
            self.w.serving_endpoints.create(name=self.ENDPOINT_NAME, config=config)
            logger.info(f"created endpoint {self.ENDPOINT_NAME} → v{version}")

    # ---- Canary deploy (split traffic) ----
    def deploy_canary(self, champion_version: str, challenger_version: str,
                      challenger_pct: int = 10):
        """Split traffic between champion and challenger versions."""
        from databricks.sdk.service.serving import ServedEntityInput, TrafficConfig, Route
        champion = ServedEntityInput(
            entity_name=self.MODEL_NAME, entity_version=champion_version,
            workload_size=self.WORKLOAD_SIZE, scale_to_zero_enabled=self.SCALE_TO_ZERO,
            name="champion")
        challenger = ServedEntityInput(
            entity_name=self.MODEL_NAME, entity_version=challenger_version,
            workload_size=self.WORKLOAD_SIZE, scale_to_zero_enabled=self.SCALE_TO_ZERO,
            name="challenger")
        traffic = TrafficConfig(routes=[
            Route(served_model_name="champion", traffic_percentage=100 - challenger_pct),
            Route(served_model_name="challenger", traffic_percentage=challenger_pct),
        ])
        self.w.serving_endpoints.update_config(
            self.ENDPOINT_NAME, served_entities=[champion, challenger], traffic_config=traffic)
        logger.info(f"canary: {100-challenger_pct}% champion(v{champion_version}) / "
                    f"{challenger_pct}% challenger(v{challenger_version})")

    # ---- Promote canary to full ----
    def promote_canary(self, challenger_version: str):
        """Shift 100% traffic to the challenger (it becomes the new champion)."""
        self.deploy(challenger_version)
        logger.info(f"promoted v{challenger_version} to 100%")

    # ---- Rollback ----
    def rollback(self, safe_version: str):
        """Instant rollback: deploy the known-good version at 100%."""
        self.deploy(safe_version)
        logger.info(f"ROLLBACK: endpoint → v{safe_version} (100%)")


if __name__ == "__main__":
    d = ModelDeploymentDatabricks({"endpoint_name": "churn-v1", "model_name": "main.ml.churn_model"})
    # d.deploy("3")                                    # full deploy
    # d.deploy_canary("2", "3", challenger_pct=10)     # 90/10 split
    # d.promote_canary("3")                            # shift to 100% new
    # d.rollback("2")                                  # instant rollback
