"""
================================================================================
EXCEPTION HIERARCHY — [AWS]
================================================================================
Purpose : Shared, typed exceptions so every job fails with a clear, catchable
          error type instead of a generic Exception. Orchestrators (Step
          Functions / Workflows) can branch on these.

Same class names exist in the Databricks tree (keep in sync) so business code
is identical across platforms.

Usage:
    from aws.src.common.exceptions.exceptions import SourceNotFoundError, ConfigError
    if not args.get("DS_BUCKET"):
        raise ConfigError("missing required arg --DS_BUCKET")
Version : 2026-06-28
================================================================================
"""


class PlatformError(Exception):
    """Base class for all platform errors."""


class ConfigError(PlatformError):
    """Missing/invalid configuration or job argument (fail fast — don't fall back)."""


class SourceNotFoundError(PlatformError):
    """An expected source (table/path/partition/file) does not exist."""


class SchemaDriftError(PlatformError):
    """Source schema changed in a way the job cannot safely absorb."""


class DQError(PlatformError):
    """A data-quality check failed at a severity that should stop the pipeline."""


class WriteError(PlatformError):
    """Failure while writing output (catalog update, partition overwrite, etc.)."""


class UpstreamNotReadyError(PlatformError):
    """Upstream layer/data not ready yet (use to trigger graceful skip/retry)."""
