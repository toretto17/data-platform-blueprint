"""
Enterprise Data Platform — Environment Configuration
=====================================================
Single source of truth for all environment-specific settings.
Each environment (dev/qa/uat/prod) has its own class.

Usage:
    from aws.src.common.constants.config import get_config
    cfg = get_config("dev")
    print(cfg.S3_SILVER_BUCKET)

Extend:
    Add new environments by creating a new class inheriting BaseConfig.
    Add new settings to BaseConfig (shared) or environment classes (specific).
"""
import os


class BaseConfig:
    """Base configuration — shared across all environments."""

    # --- Core Identity (MUST override in env classes) ---
    PROJECT = "CHANGE_ME"           # e.g., "bnic", "myco"
    FEATURE = "CHANGE_ME"           # e.g., "aii", "revenue"
    DOMAIN = "CHANGE_ME"            # e.g., "mobile_revenue_analytics"
    ENVIRONMENT = "CHANGE_ME"       # e.g., "dev", "prod"
    ACCOUNT_ID = "CHANGE_ME"        # e.g., "123456789012"
    AWS_REGION = "ap-southeast-1"

    # --- S3 Buckets (derived from identity) ---
    @property
    def BUCKET_PREFIX(self):
        return f"s3-{self.PROJECT}-{self.FEATURE}-{self.ENVIRONMENT}"

    @property
    def S3_SILVER_BUCKET(self):
        return f"{self.BUCKET_PREFIX}-silver-{self.ACCOUNT_ID}"

    @property
    def S3_GOLD_BUCKET(self):
        return f"{self.BUCKET_PREFIX}-gold-{self.ACCOUNT_ID}"

    @property
    def S3_CONSUMPTION_BUCKET(self):
        return f"{self.BUCKET_PREFIX}-consumption-{self.ACCOUNT_ID}"

    @property
    def S3_DQ_BUCKET(self):
        return f"{self.BUCKET_PREFIX}-data-quality-{self.ACCOUNT_ID}"

    @property
    def S3_ARTIFACTORY_BUCKET(self):
        return f"{self.BUCKET_PREFIX}-artifactory-{self.ACCOUNT_ID}"

    @property
    def S3_FEATURE_BUCKET(self):
        return f"{self.BUCKET_PREFIX}-feature-{self.ACCOUNT_ID}"

    # --- Databases ---
    @property
    def DB_SILVER(self):
        return f"{self.DOMAIN}_silver"

    @property
    def DB_GOLD(self):
        return f"{self.DOMAIN}_gold"

    DB_CONSUMPTION = "insights_consumption_layer"

    # --- IAM Roles ---
    @property
    def GLUE_ROLE_ARN(self):
        return f"arn:aws:iam::{self.ACCOUNT_ID}:role/GlueServiceRole-{self.ENVIRONMENT}-{self.PROJECT}"

    @property
    def SAGEMAKER_ROLE_ARN(self):
        return f"arn:aws:iam::{self.ACCOUNT_ID}:role/iam-{self.PROJECT}-mlops-{self.ENVIRONMENT}-sagemaker-default-execution-role"

    # --- DynamoDB ---
    @property
    def DDB_CONFIG_TABLE(self):
        return f"{self.PROJECT}-{self.FEATURE}-fw-config-table"

    # --- Step Functions ---
    @property
    def FRAMEWORK_SF_ARN(self):
        return f"arn:aws:states:{self.AWS_REGION}:{self.ACCOUNT_ID}:stateMachine:sfn-{self.PROJECT}-{self.FEATURE}-fw-transformation"

    # --- Data Quality ---
    DQ_SETTINGS = {
        "enable_cloudwatch_metrics": True,
        "max_retries": 3,
        "default_min_row_count": 1000,
    }

    # --- Spark Defaults ---
    SPARK_DEFAULTS = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
    }


class DevConfig(BaseConfig):
    """Development environment."""
    ENVIRONMENT = "dev"
    ACCOUNT_ID = "CHANGE_ME"  # e.g., "503561443692"

    DQ_SETTINGS = {**BaseConfig.DQ_SETTINGS, "default_min_row_count": 100}


class QAConfig(BaseConfig):
    """QA/Testing environment."""
    ENVIRONMENT = "qa"
    ACCOUNT_ID = "CHANGE_ME"


class UATConfig(BaseConfig):
    """UAT environment."""
    ENVIRONMENT = "uat"
    ACCOUNT_ID = "CHANGE_ME"


class ProdConfig(BaseConfig):
    """Production environment."""
    ENVIRONMENT = "prod"
    ACCOUNT_ID = "CHANGE_ME"  # e.g., "127214173492"

    DQ_SETTINGS = {**BaseConfig.DQ_SETTINGS, "max_retries": 5}


# --- Factory ---
_CONFIGS = {"dev": DevConfig, "qa": QAConfig, "uat": UATConfig, "prod": ProdConfig}


def get_config(environment: str = None) -> BaseConfig:
    """Get config for environment. Reads ENV var if not passed."""
    env = environment or os.getenv("ENVIRONMENT", "dev")
    if env not in _CONFIGS:
        raise ValueError(f"Unknown environment: {env}. Available: {list(_CONFIGS)}")
    return _CONFIGS[env]()


def get_account_id(env_slug: str = "dev") -> str:
    """Get account ID for an environment slug."""
    return get_config(env_slug).ACCOUNT_ID
