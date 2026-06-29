"""
================================================================================
EXCEPTION HIERARCHY — [Databricks]
================================================================================
Purpose : Identical exception types as the AWS tree (keep in sync) so business
          code is the same across platforms. Workflows can branch on these.

Usage:
    from databricks.src.common.exceptions.exceptions import ConfigError
    if not cfg.get("catalog"):
        raise ConfigError("missing required config: catalog")
Version : 2026-06-28
================================================================================
"""


class PlatformError(Exception):
    """Base class for all platform errors."""


class ConfigError(PlatformError):
    """Missing/invalid configuration or job parameter (fail fast — don't fall back)."""


class SourceNotFoundError(PlatformError):
    """An expected source (table/path/partition/file) does not exist."""


class SchemaDriftError(PlatformError):
    """Source schema changed in a way the job cannot safely absorb."""


class DQError(PlatformError):
    """A data-quality check failed at a severity that should stop the pipeline."""


class WriteError(PlatformError):
    """Failure while writing output (Delta MERGE, table create, etc.)."""


class UpstreamNotReadyError(PlatformError):
    """Upstream layer/data not ready yet (use to trigger graceful skip/retry)."""
