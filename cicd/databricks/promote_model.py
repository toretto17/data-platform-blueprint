"""
Model Promotion — Unity Catalog Champion/Challenger Alias Management
=====================================================================

PURPOSE:
    Promotes a model version from one alias to another in Unity Catalog.
    Typical flow: Challenger → Champion (after validation passes).

    This replaces the legacy Staging/Production model stages with the
    modern UC alias-based approach (recommended by Databricks since 2024).

PATTERN:
    1. Training job registers new version → assigns "Challenger" alias
    2. Validation job runs quality checks on Challenger
    3. If checks pass → this script promotes Challenger → Champion
    4. All inference workloads reference @Champion → automatically pick up new version

UC MODEL ALIASES:
    - "Champion": The production model. Inference jobs use model_name@Champion.
    - "Challenger": The candidate model under evaluation.
    - Custom aliases: You can add "Archived", "RollbackTarget", etc.

    Key API: mlflow.client.MlflowClient().set_registered_model_alias()

WHAT TO CHANGE:
    1. Validation logic in validate_challenger() — your quality gates
    2. Alert/notification after promotion (Slack, email, etc.)
    3. Rollback logic if needed (store previous Champion version)

USAGE:
    # As Databricks notebook (called by promotion job in ml_bundle.yml):
    # Parameters passed via base_parameters:
    #   env: dev/staging/prod
    #   model_name: catalog.schema.model_name
    #   from_alias: Challenger
    #   to_alias: Champion

    # As standalone script:
    python promote_model.py --model-name catalog.schema.my_model --from-alias Challenger --to-alias Champion

PREREQUISITES:
    - MLflow 2.9+ with Unity Catalog support
    - Model already registered with from_alias assigned
    - Caller has MANAGE permission on the UC model
    - Databricks Runtime 14.0+ or MLflow client configured for UC

DOCUMENTATION:
    - Unity Catalog Models: https://docs.databricks.com/machine-learning/manage-model-lifecycle/
    - Model Aliases: https://mlflow.org/docs/latest/model-registry/
    - copy_model_version: https://mlflow.org/docs/latest/python_api/mlflow.client.html#mlflow.client.MlflowClient.copy_model_version
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone

import mlflow
from mlflow.tracking import MlflowClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION — CHANGE THESE
# ═══════════════════════════════════════════════════════════════════════════════

# Minimum metric thresholds to allow promotion (safety gate)
# CHANGE: Set your model's quality requirements
PROMOTION_GATES = {
    "min_accuracy": 0.85,        # CHANGE_ME: minimum accuracy to promote
    "max_rmse": 0.15,            # CHANGE_ME: maximum RMSE to promote
    "min_f1": 0.80,              # CHANGE_ME: minimum F1 score
    "max_mape": 0.20,            # CHANGE_ME: maximum MAPE (for forecasting)
}

# Whether to enforce validation before promotion
# Set False to allow manual promotion without checks (escape hatch)
ENFORCE_VALIDATION = True

# ═══════════════════════════════════════════════════════════════════════════════


def parse_args():
    """Parse arguments (from notebook base_parameters or CLI)."""
    parser = argparse.ArgumentParser(description="Promote ML model alias in Unity Catalog")
    parser.add_argument("--model-name", required=True, help="Full UC model name: catalog.schema.model")
    parser.add_argument("--from-alias", default="Challenger", help="Source alias to promote from")
    parser.add_argument("--to-alias", default="Champion", help="Target alias to promote to")
    parser.add_argument("--env", default="dev", help="Environment (dev/staging/prod)")
    parser.add_argument("--skip-validation", action="store_true", help="Skip quality gate checks")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen without executing")
    return parser.parse_args()


def get_model_version_by_alias(client: MlflowClient, model_name: str, alias: str) -> int | None:
    """
    Get the model version number currently assigned to an alias.

    Returns:
        int: Version number, or None if alias not assigned.
    """
    try:
        mv = client.get_model_version_by_alias(name=model_name, alias=alias)
        return int(mv.version)
    except mlflow.exceptions.MlflowException as e:
        if "RESOURCE_DOES_NOT_EXIST" in str(e) or "NOT_FOUND" in str(e):
            logger.info("Alias '%s' not currently assigned on model '%s'", alias, model_name)
            return None
        raise


def get_model_version_metrics(client: MlflowClient, model_name: str, version: int) -> dict:
    """
    Retrieve metrics from the MLflow run that produced this model version.

    Returns:
        dict: Run metrics (e.g., {"accuracy": 0.92, "rmse": 0.08, "f1": 0.89})
    """
    mv = client.get_model_version(name=model_name, version=str(version))
    run_id = mv.run_id

    if not run_id:
        logger.warning("Model version %d has no associated run — cannot retrieve metrics", version)
        return {}

    run = client.get_run(run_id)
    metrics = run.data.metrics
    logger.info("Version %d metrics: %s", version, json.dumps(metrics, default=str))
    return metrics


def validate_challenger(metrics: dict) -> tuple[bool, list[str]]:
    """
    Validate model metrics against promotion gates.

    CHANGE: Update PROMOTION_GATES and this logic for YOUR model type.
    Different model types (classification, regression, forecasting) need
    different metrics and thresholds.

    Returns:
        tuple: (passes: bool, violations: list[str])
    """
    violations = []

    # Check each gate
    if "accuracy" in metrics and metrics["accuracy"] < PROMOTION_GATES.get("min_accuracy", 0):
        violations.append(f"accuracy={metrics['accuracy']:.4f} < min={PROMOTION_GATES['min_accuracy']}")

    if "rmse" in metrics and metrics["rmse"] > PROMOTION_GATES.get("max_rmse", float("inf")):
        violations.append(f"rmse={metrics['rmse']:.4f} > max={PROMOTION_GATES['max_rmse']}")

    if "f1" in metrics and metrics["f1"] < PROMOTION_GATES.get("min_f1", 0):
        violations.append(f"f1={metrics['f1']:.4f} < min={PROMOTION_GATES['min_f1']}")

    if "mape" in metrics and metrics["mape"] > PROMOTION_GATES.get("max_mape", float("inf")):
        violations.append(f"mape={metrics['mape']:.4f} > max={PROMOTION_GATES['max_mape']}")

    passes = len(violations) == 0
    return passes, violations


def compare_with_champion(
    client: MlflowClient,
    model_name: str,
    challenger_version: int,
    champion_version: int | None,
) -> dict:
    """
    Compare Challenger metrics against current Champion.

    Returns:
        dict: Comparison results {metric: {challenger: x, champion: y, improved: bool}}
    """
    if champion_version is None:
        logger.info("No current Champion — Challenger will be first Champion")
        return {}

    challenger_metrics = get_model_version_metrics(client, model_name, challenger_version)
    champion_metrics = get_model_version_metrics(client, model_name, champion_version)

    comparison = {}
    for metric in set(challenger_metrics.keys()) & set(champion_metrics.keys()):
        c_val = challenger_metrics[metric]
        ch_val = champion_metrics[metric]

        # CHANGE: Define which metrics are "higher is better" vs "lower is better"
        higher_is_better = metric in ("accuracy", "f1", "auc", "r2")
        improved = c_val > ch_val if higher_is_better else c_val < ch_val

        comparison[metric] = {
            "challenger": round(c_val, 4),
            "champion": round(ch_val, 4),
            "improved": improved,
        }

    logger.info("Comparison vs Champion:\n%s", json.dumps(comparison, indent=2))
    return comparison


def promote(
    client: MlflowClient,
    model_name: str,
    version: int,
    from_alias: str,
    to_alias: str,
    dry_run: bool = False,
):
    """
    Promote a model version: assign to_alias, optionally remove from_alias.

    This is the core promotion action:
      1. Assign to_alias (e.g., "Champion") to the version
      2. The previous to_alias holder automatically loses the alias
      3. Optionally archive the previous Champion

    Inference workloads using model_name@Champion will immediately
    pick up the new version on next load.
    """
    if dry_run:
        logger.info("[DRY-RUN] Would promote version %d: %s → %s", version, from_alias, to_alias)
        return

    # Assign the new alias
    client.set_registered_model_alias(
        name=model_name,
        alias=to_alias,
        version=str(version),
    )
    logger.info("✅ Promoted version %d to alias '%s'", version, to_alias)

    # Optionally tag the version with promotion metadata
    client.set_model_version_tag(
        name=model_name,
        version=str(version),
        key="promoted_at",
        value=datetime.now(timezone.utc).isoformat(),
    )
    client.set_model_version_tag(
        name=model_name,
        version=str(version),
        key="promoted_from",
        value=from_alias,
    )

    # Remove the from_alias (Challenger) — version is now Champion only
    try:
        client.delete_registered_model_alias(name=model_name, alias=from_alias)
        logger.info("Removed alias '%s' from version %d (now only '%s')", from_alias, version, to_alias)
    except Exception:
        # Non-critical — alias may not exist if version was promoted directly
        pass


def main():
    args = parse_args()
    logger.info("═══ Model Promotion: %s ═══", args.model_name)
    logger.info("From: @%s → To: @%s (env=%s)", args.from_alias, args.to_alias, args.env)

    # Initialize MLflow client (auto-configures for Databricks UC)
    client = MlflowClient()

    # Step 1: Find the version with from_alias (Challenger)
    challenger_version = get_model_version_by_alias(client, args.model_name, args.from_alias)
    if challenger_version is None:
        logger.error("No version found with alias '%s' — nothing to promote", args.from_alias)
        sys.exit(1)
    logger.info("Challenger version: %d", challenger_version)

    # Step 2: Find current Champion (for comparison)
    champion_version = get_model_version_by_alias(client, args.model_name, args.to_alias)
    if champion_version:
        logger.info("Current Champion version: %d", champion_version)
    else:
        logger.info("No current Champion — this will be the first")

    # Step 3: Validate Challenger quality gates
    if ENFORCE_VALIDATION and not args.skip_validation:
        metrics = get_model_version_metrics(client, args.model_name, challenger_version)
        passes, violations = validate_challenger(metrics)

        if not passes:
            logger.error("❌ Challenger FAILS quality gates:")
            for v in violations:
                logger.error("   • %s", v)
            logger.error("Promotion BLOCKED. Fix model or use --skip-validation to override.")
            sys.exit(1)

        logger.info("✅ Challenger passes all quality gates")

    # Step 4: Compare with Champion (informational)
    if champion_version:
        compare_with_champion(client, args.model_name, challenger_version, champion_version)

    # Step 5: Promote
    promote(
        client=client,
        model_name=args.model_name,
        version=challenger_version,
        from_alias=args.from_alias,
        to_alias=args.to_alias,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        logger.info("[DRY-RUN] No changes made")
    else:
        logger.info("═══ Promotion complete: v%d is now @%s ═══", challenger_version, args.to_alias)

    # Step 6: Optional — trigger downstream (notify, update serving endpoint, etc.)
    # CHANGE: Add your notification logic here
    # Example: post to Slack, trigger serving endpoint update, update DDB config
    # if not args.dry_run:
    #     notify_team(model_name=args.model_name, version=challenger_version, alias=args.to_alias)


# Databricks notebook entry point
# When run as a notebook, parameters come from dbutils.widgets
try:
    import dbutils  # noqa: F401 — only available in Databricks

    # Read parameters from notebook widgets (set by job base_parameters)
    env = dbutils.widgets.get("env")  # type: ignore
    model_name = dbutils.widgets.get("model_name")  # type: ignore
    from_alias = dbutils.widgets.get("from_alias")  # type: ignore
    to_alias = dbutils.widgets.get("to_alias")  # type: ignore

    # Override sys.argv for argparse compatibility
    sys.argv = [
        "promote_model.py",
        "--model-name", model_name,
        "--from-alias", from_alias,
        "--to-alias", to_alias,
        "--env", env,
    ]
    main()
except (ImportError, Exception):
    # Running outside Databricks (CLI or CI/CD)
    if __name__ == "__main__":
        main()
