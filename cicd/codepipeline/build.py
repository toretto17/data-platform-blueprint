"""
ML Model Promotion Script (build.py)
=====================================

PURPOSE:
    Promotes an approved SageMaker model from a source account (nonprod/training)
    to a target account (nonprod-staging or prod). This is called by the DEPLOY
    buildspec for EACH model package group.

WHAT IT DOES:
    1. Finds the latest Approved ModelPackage in the source account
    2. Copies model artifacts (model.tar.gz) from source S3 to target S3
    3. Creates the ModelPackageGroup in target if it doesn't exist (idempotent)
    4. Registers a new ModelPackage version in the target account
    5. Writes Terraform tfvars for endpoint provisioning (optional)

WHAT TO CHANGE:
    - ARTIFACT_BUCKET_PATTERN: Your S3 bucket naming convention for model artifacts
    - MODEL_DATA_PREFIX: Your S3 prefix where model.tar.gz lives
    - _group_tags(): Your cost-attribution tags
    - ensure_model_package_group(): Your group description format
    - INFERENCE_SPEC: Your container image URI and supported content types

USAGE:
    python3 build.py \\
        --sagemaker-project-name "my-project" \\
        --sagemaker-project-id "p-xxx" \\
        --model-package-group-name "MyModel-ProductA-GA" \\
        --sagemaker-project-arn "arn:aws:sagemaker:..." \\
        --stage prod \\
        --training-id 123456789012 \\
        --target-id 987654321098 \\
        --environment prod \\
        --code tf

PREREQUISITES:
    - boto3 installed
    - AWS credentials assumed into TARGET account (done by buildspec pre_build)
    - Source account S3 bucket allows cross-account GetObject from target role
    - Target account has SageMaker execution role for inference

ACCOUNTS:
    - Source (training-id): Where models are trained and Approved
    - Target (target-id): Where models are deployed for inference
    - If same account (nonprod → nonprod): source = target, no cross-account copy needed
"""

import argparse
import json
import logging
import os
import sys
import tempfile
import urllib.parse

import boto3
from botocore.exceptions import ClientError

LOG_FORMAT = "%(levelname)s: [%(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


# ─── CHANGE THESE ───────────────────────────────────────────────────────────
# Your S3 bucket naming pattern for model artifacts.
# {account_id} and {env} are substituted at runtime.
ARTIFACT_BUCKET_PATTERN = "s3-CHANGE_ME-mlops-{env}-artifact-bucket-{account_id}"

# S3 prefix where SageMaker stores model.tar.gz after training
# Example: "model-artifacts/{group_name}/{version}/"
MODEL_DATA_PREFIX = "CHANGE_ME/model-artifacts"

# Your inference container image URI (ECR)
# CHANGE: Replace with your ECR image URI pattern
INFERENCE_IMAGE_URI_PATTERN = (
    "{account_id}.dkr.ecr.{region}.amazonaws.com/CHANGE_ME-{model_family}:{tag}"
)

# Default inference image tag
DEFAULT_IMAGE_TAG = "latest"

# AWS region
REGION = os.environ.get("AWS_REGION", "us-east-1")  # CHANGE_ME: your default region
# ────────────────────────────────────────────────────────────────────────────


def parse_args():
    """Parse command-line arguments from buildspec."""
    parser = argparse.ArgumentParser(description="Promote ML model to target account")
    parser.add_argument("--sagemaker-project-name", required=True, help="SageMaker Project name")
    parser.add_argument("--sagemaker-project-id", required=True, help="SageMaker Project ID")
    parser.add_argument("--model-package-group-name", required=True, help="Model Package Group to promote")
    parser.add_argument("--sagemaker-project-arn", required=True, help="SageMaker Project ARN")
    parser.add_argument("--stage", required=True, choices=["nonprod", "prod"], help="Target stage")
    parser.add_argument("--training-id", required=True, help="Source (training) account ID")
    parser.add_argument("--target-id", required=True, help="Target (deploy) account ID")
    parser.add_argument("--environment", required=True, help="Target environment name")
    parser.add_argument("--code", default="tf", help="Output format: tf (terraform tfvars)")
    return parser.parse_args()


def get_sm_client(region=REGION):
    """Get SageMaker client using current credentials (already assumed into target)."""
    return boto3.client("sagemaker", region_name=region)


def get_s3_client(region=REGION):
    """Get S3 client."""
    return boto3.client("s3", region_name=region)


def get_approved_package(model_package_group_name: str, sm_client) -> tuple:
    """
    Find the latest Approved model package in the source account.

    Returns:
        tuple: (package_arn, package_version)

    Raises:
        Exception: If no approved package exists for this group.
    """
    try:
        approved_packages = []
        for page in sm_client.get_paginator("list_model_packages").paginate(
            ModelPackageGroupName=model_package_group_name,
            ModelApprovalStatus="Approved",
            SortBy="CreationTime",
            SortOrder="Descending",
        ):
            approved_packages.extend(page["ModelPackageSummaryList"])

        if not approved_packages:
            logger.error(
                "No approved ModelPackage found for group: %s",
                model_package_group_name,
            )
            raise Exception(f"No approved ModelPackage found for {model_package_group_name}")

        package_arn = approved_packages[0]["ModelPackageArn"]
        package_version = approved_packages[0].get("ModelPackageVersion", 1)
        logger.info("Latest approved package: %s (version %s)", package_arn, package_version)
        return package_arn, package_version

    except ClientError as e:
        logger.error("AWS error listing packages: %s", e.response["Error"]["Message"])
        raise


def _group_tags(group_name: str, args) -> list:
    """
    Tags for the ModelPackageGroup in the target account.
    Used for cost attribution and project identification.

    CHANGE: Update tag keys/values to match your organization's tagging strategy.
    """
    # Derive cost tag from group name pattern
    # CHANGE: Your logic for determining cost center from group name
    cost_tag = "ml-inference"  # CHANGE_ME

    return [
        {"Key": "sagemaker:project-name", "Value": args.sagemaker_project_name},
        {"Key": "sagemaker:project-id", "Value": args.sagemaker_project_id},
        {"Key": "pipeline-cost-tag", "Value": cost_tag},
        {"Key": "environment", "Value": args.environment},
    ]


def ensure_model_package_group(sm_client, group_name: str, tags: list = None):
    """
    Create ModelPackageGroup in target account if it doesn't exist.
    Idempotent — safe to call on every deploy.

    WHY: Versioned model packages (inside a group) support CustomerMetadataProperties
    which are needed for monitoring configuration. Groups also allow tagging for cost.
    """
    try:
        resp = sm_client.describe_model_package_group(ModelPackageGroupName=group_name)
        logger.info("Model package group '%s' already exists in target", group_name)
        # Refresh tags on existing group (in case tagging policy changed)
        if tags:
            sm_client.add_tags(ResourceArn=resp["ModelPackageGroupArn"], Tags=tags)
    except ClientError:
        # Group doesn't exist — create it
        kwargs = {
            "ModelPackageGroupName": group_name,
            "ModelPackageGroupDescription": f"Promoted from nonprod: {group_name}",
        }
        if tags:
            kwargs["Tags"] = tags
        sm_client.create_model_package_group(**kwargs)
        logger.info("Created model package group '%s' in target", group_name)


def copy_model_artifacts(source_s3_uri: str, target_bucket: str, target_key: str, s3_client):
    """
    Copy model artifacts from source to target S3 location.

    For cross-account copy:
      - Source bucket must have a bucket policy allowing s3:GetObject from target role
      - OR use a two-step: download to CodeBuild local → upload to target

    This implementation uses the two-step approach (works regardless of bucket policy).
    For same-account promotion, this is a simple S3 copy.
    """
    # Parse source URI
    parsed = urllib.parse.urlparse(source_s3_uri)
    source_bucket = parsed.netloc
    source_key = parsed.path.lstrip("/")

    logger.info("Copying artifacts: s3://%s/%s → s3://%s/%s", source_bucket, source_key, target_bucket, target_key)

    try:
        # Try direct copy (works if bucket policy allows cross-account)
        s3_client.copy(
            CopySource={"Bucket": source_bucket, "Key": source_key},
            Bucket=target_bucket,
            Key=target_key,
        )
        logger.info("Direct S3 copy succeeded")
    except ClientError:
        # Fallback: download + upload (always works if CodeBuild has access to both)
        logger.info("Direct copy failed, using download+upload fallback")
        with tempfile.NamedTemporaryFile() as tmp:
            s3_client.download_file(source_bucket, source_key, tmp.name)
            s3_client.upload_file(tmp.name, target_bucket, target_key)
        logger.info("Download+upload fallback succeeded")


def register_model_in_target(
    sm_client,
    group_name: str,
    model_data_url: str,
    image_uri: str,
    source_package_arn: str,
    args,
):
    """
    Register a new ModelPackage version in the target account.

    This creates a deployable model artifact that SageMaker Batch Transform
    or real-time endpoints can use.

    CHANGE: Update InferenceSpecification to match your model's requirements
    (supported content types, instance types, etc.)
    """
    inference_spec = {
        "Containers": [
            {
                "Image": image_uri,
                "ModelDataUrl": model_data_url,
                # CHANGE: Add Framework/FrameworkVersion if using built-in algorithms
            }
        ],
        "SupportedContentTypes": ["application/x-parquet", "text/csv"],  # CHANGE
        "SupportedResponseMIMETypes": ["application/x-parquet", "text/csv"],  # CHANGE
        "SupportedTransformInstanceTypes": ["ml.m5.xlarge", "ml.m5.2xlarge"],  # CHANGE
    }

    # CustomerMetadataProperties — used by monitoring setup
    customer_metadata = {
        "source_package_arn": source_package_arn,
        "promoted_from": args.training_id,
        "promoted_to": args.target_id,
        "environment": args.environment,
    }

    try:
        response = sm_client.create_model_package(
            ModelPackageGroupName=group_name,
            InferenceSpecification=inference_spec,
            ModelApprovalStatus="Approved",  # Auto-approve in target (already approved in source)
            ModelPackageDescription=f"Promoted from {args.training_id} to {args.target_id}",
            CustomerMetadataProperties=customer_metadata,
        )
        logger.info("Registered model package: %s", response["ModelPackageArn"])
        return response["ModelPackageArn"]
    except ClientError as e:
        logger.error("Failed to register model: %s", e.response["Error"]["Message"])
        raise


def write_tfvars(group_name: str, model_package_arn: str, args):
    """
    Write Terraform tfvars for endpoint provisioning.
    Terraform can then create/update the SageMaker endpoint using these values.

    CHANGE: Adjust the tfvars structure to match your Terraform module inputs.
    """
    tfvars = {
        "model_package_arn": model_package_arn,
        "model_package_group_name": group_name,
        "environment": args.environment,
        "instance_type": "ml.m5.xlarge",  # CHANGE: your default instance type
        "instance_count": 1,
    }

    output_dir = os.path.join(os.path.dirname(__file__), "realtime") if "__file__" in dir() else "."
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"config-{group_name}.tfvars")

    with open(output_path, "w") as f:
        for k, v in tfvars.items():
            if isinstance(v, str):
                f.write(f'{k} = "{v}"\n')
            else:
                f.write(f"{k} = {v}\n")

    logger.info("Wrote tfvars: %s", output_path)


def rewrite_nonprod_refs(value: str, source_account_id: str, target_account_id: str, target_env: str) -> str:
    """
    Rewrite S3 URIs and bucket names from nonprod (source) to target (prod).

    WHY THIS IS NEEDED:
        Model packages store S3 URIs for artifacts, baselines, evaluation results.
        When we promote cross-account, these URIs still point to nonprod buckets.
        Prod inference/monitoring must read from prod-owned buckets.

    WHAT IT REWRITES:
        1. Account ID: "111111111111" → "222222222222" (source → target)
        2. Environment slug: "-nonprod-" → "-prod-" in bucket names
        3. Environment prefix: "dev-" → "prod-" in resource names

    CHANGE: Update the replacement patterns to match YOUR bucket naming convention.
    Common patterns:
        - "s3-project-nonprod-bucket-{account}" → "s3-project-prod-bucket-{account}"
        - "project-{env}-data-{account}" → "project-{env}-data-{account}"

    Args:
        value: String containing S3 URIs or bucket references
        source_account_id: Nonprod account ID to replace
        target_account_id: Prod account ID to use
        target_env: Target environment name ("prod")

    Returns:
        Rewritten string with all nonprod references replaced
    """
    if not value:
        return value

    result = value

    # Replace account ID
    result = result.replace(source_account_id, target_account_id)

    # Replace environment slug in bucket names
    # CHANGE: Add your specific patterns here
    result = result.replace("-nonprod-", f"-{target_env}-")
    result = result.replace("-dev-", f"-{target_env}-")

    return result


def wait_for_model_package(sm_client, model_package_arn: str, timeout_seconds: int = 300):
    """
    Poll until ModelPackage status is Completed/InProgress→Completed.

    WHY: For large models, create_model_package is async. The package needs
    to reach 'Completed' status before it can be used by Batch Transform or endpoints.
    Most registrations are instant, but this handles edge cases.

    Args:
        sm_client: boto3 SageMaker client
        model_package_arn: ARN of the package to wait for
        timeout_seconds: Max wait time (default 5 minutes)
    """
    import time

    start = time.time()
    while True:
        resp = sm_client.describe_model_package(ModelPackageName=model_package_arn)
        status = resp.get("ModelPackageStatus", "Unknown")

        if status == "Completed":
            logger.info("ModelPackage %s → Completed", model_package_arn.split("/")[-1])
            return
        elif status in ("Failed", "Deleting"):
            raise Exception(f"ModelPackage {model_package_arn} reached terminal status: {status}")

        elapsed = time.time() - start
        if elapsed > timeout_seconds:
            logger.warning(
                "ModelPackage still in '%s' after %ds — proceeding anyway (may be usable)",
                status, int(elapsed),
            )
            return

        logger.info("ModelPackage status: %s — waiting... (%.0fs elapsed)", status, elapsed)
        time.sleep(10)


def write_monitoring_ddb_row(
    group_name: str,
    model_package_arn: str,
    args,
    sm_client,
    source_account_id: str,
):
    """
    Mirror the monitoring config from nonprod DDB to prod DDB.

    FLOW:
        1. Read monitoring row from nonprod DDB (source of truth for schedules, thresholds)
        2. Rewrite all nonprod bucket/account references to prod
        3. Write to prod DDB
        4. If nonprod row doesn't exist → use _monitoring_defaults.py to generate safe defaults

    WHY:
        The monitoring Step Function reads its config from DDB. When promoting to prod,
        we need the monitoring row to exist in prod DDB with prod-appropriate values.

    CHANGE:
        - MONITORING_DDB_TABLE: Your monitoring DynamoDB table name pattern
        - DDB key schema: Match your table's partition/sort key
        - Fields to rewrite: Add any project-specific URI fields

    Args:
        group_name: ModelPackageGroup name
        model_package_arn: ARN of the newly registered package in target
        args: CLI args (stage, training_id, target_id, environment)
        sm_client: SageMaker client (for _monitoring_defaults lookups)
        source_account_id: Nonprod account ID (for reading source DDB)
    """
    # CHANGE: Your monitoring DDB table name pattern
    # Example: "tbl-CHANGE_ME-mlops-{env}-monitoring-config"
    MONITORING_DDB_TABLE = "tbl-CHANGE_ME-mlops-{env}-monitoring-config"

    source_table_name = MONITORING_DDB_TABLE.format(env="nonprod")
    target_table_name = MONITORING_DDB_TABLE.format(env=args.environment)

    # We need nonprod creds to read source table, and target creds to write.
    # At this point in the buildspec, we're assumed into TARGET account.
    # For cross-account DDB read, we'd need to assume back or use a different pattern.
    #
    # SIMPLIFICATION: If your monitoring DDB is in the same account as the models,
    # just read from target (it was seeded by register.py during training).
    # If truly cross-account, you'll need a second assume-role here.

    ddb_client = boto3.client("dynamodb", region_name=REGION)

    try:
        # Try to read existing row from target DDB (may have been seeded by register.py)
        response = ddb_client.get_item(
            TableName=target_table_name,
            Key={
                # CHANGE: Your DDB partition key structure
                "model_package_group": {"S": group_name},
            },
        )
        item = response.get("Item")

        if item:
            logger.info("Monitoring row already exists in %s for %s — updating package ARN only", target_table_name, group_name)
            # Update the package ARN to the newly promoted version
            ddb_client.update_item(
                TableName=target_table_name,
                Key={"model_package_group": {"S": group_name}},
                UpdateExpression="SET latest_package_arn = :arn",
                ExpressionAttributeValues={":arn": {"S": model_package_arn}},
            )
            return

    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            logger.warning("DDB read failed: %s — attempting to build defaults", e)

    # Fallback: build default row using _monitoring_defaults.py
    logger.info("No existing monitoring row for %s — building from defaults", group_name)
    try:
        from _monitoring_defaults import build_default_monitoring_row

        default_row = build_default_monitoring_row(
            group_name=group_name,
            sm_client=sm_client,
            environment=args.environment,
        )

        if default_row:
            # Add the package ARN
            default_row["latest_package_arn"] = model_package_arn

            # Write to DDB (convert to DynamoDB format)
            ddb_item = {}
            for k, v in default_row.items():
                if isinstance(v, str):
                    ddb_item[k] = {"S": v}
                elif isinstance(v, (int, float, Decimal)):
                    ddb_item[k] = {"N": str(v)}
                elif isinstance(v, list):
                    ddb_item[k] = {"L": [{"S": str(i)} for i in v]}
                elif isinstance(v, bool):
                    ddb_item[k] = {"BOOL": v}

            ddb_client.put_item(TableName=target_table_name, Item=ddb_item)
            logger.info("Wrote default monitoring row to %s for %s", target_table_name, group_name)
        else:
            logger.warning(
                "Could not build default monitoring row for %s — "
                "monitoring will not auto-configure. Run register.py manually.",
                group_name,
            )

    except ImportError:
        logger.warning(
            "_monitoring_defaults.py not found in deploy repo — "
            "monitoring DDB row NOT written. Ensure register.py has been run."
        )
    except Exception as e:
        logger.warning("Failed to write monitoring DDB row: %s (non-blocking)", e)


def main():
    args = parse_args()
    sm_client = get_sm_client()
    s3_client = get_s3_client()

    group_name = args.model_package_group_name
    logger.info("═══ Promoting: %s (stage=%s) ═══", group_name, args.stage)

    # Step 1: Ensure target has the ModelPackageGroup
    tags = _group_tags(group_name, args)
    ensure_model_package_group(sm_client, group_name, tags)

    # Step 2: Find latest approved package in source
    # NOTE: If cross-account, you may need source-account creds here.
    # In our pattern, the BUILD repo already has the source package ARN
    # passed via the buildspec loop. Adjust if your flow differs.
    source_arn, version = get_approved_package(group_name, sm_client)

    # Step 3: Get source model data URL and rewrite for target
    pkg_details = sm_client.describe_model_package(ModelPackageName=source_arn)
    source_model_url = pkg_details["InferenceSpecification"]["Containers"][0]["ModelDataUrl"]
    image_uri = pkg_details["InferenceSpecification"]["Containers"][0]["Image"]

    # Step 4: Copy artifacts to target bucket (if cross-account)
    if args.training_id != args.target_id:
        # CHANGE: Your target bucket name pattern
        target_bucket = ARTIFACT_BUCKET_PATTERN.format(
            account_id=args.target_id, env=args.environment
        )
        target_key = f"{MODEL_DATA_PREFIX}/{group_name}/v{version}/model.tar.gz"
        copy_model_artifacts(source_model_url, target_bucket, target_key, s3_client)
        target_model_url = f"s3://{target_bucket}/{target_key}"

        # Rewrite image URI if it references the source account ECR
        image_uri = rewrite_nonprod_refs(image_uri, args.training_id, args.target_id, args.environment)
    else:
        # Same account — no copy needed
        target_model_url = source_model_url

    # Step 5: Register in target
    new_arn = register_model_in_target(
        sm_client, group_name, target_model_url, image_uri, source_arn, args
    )

    # Step 6: Wait for model package to reach Completed status
    wait_for_model_package(sm_client, new_arn, timeout_seconds=300)

    # Step 7: Write monitoring DDB row (mirror nonprod config or generate defaults)
    write_monitoring_ddb_row(
        group_name=group_name,
        model_package_arn=new_arn,
        args=args,
        sm_client=sm_client,
        source_account_id=args.training_id,
    )

    # Step 8: Write tfvars (optional, for Terraform-managed endpoints)
    if args.code == "tf":
        write_tfvars(group_name, new_arn, args)

    logger.info("═══ Promotion complete: %s → %s ═══", group_name, args.stage)


if __name__ == "__main__":
    main()
