"""
================================================================================
SECRETS HELPER — [Databricks]
================================================================================
Purpose: Twin of aws/src/common/secrets/secrets.py. Fetches secrets from
         Databricks secret scopes via dbutils.secrets. Never hardcode creds.

Note: `dbutils` is injected in Databricks notebooks/jobs. Secret values are
redacted in logs/output automatically by the platform.

Set up a scope once (Databricks CLI):
    databricks secrets create-scope <scope>
    databricks secrets put-secret <scope> <key>

Usage:
    from databricks.src.common.secrets.secrets import Secrets
    sec = Secrets()
    pwd  = sec.get("prod-db", "password")
    creds = sec.get_many("prod-db", ["username", "password"])
Version : 2026-06-28
================================================================================
"""
import logging
from typing import Dict, List

logger = logging.getLogger("secrets_databricks")


class Secrets:
    def __init__(self):
        # dbutils is available in the Databricks runtime; resolve it lazily.
        try:
            self._dbutils = dbutils  # noqa: F821 (injected)
        except NameError:
            self._dbutils = self._get_dbutils()

    @staticmethod
    def _get_dbutils():
        """Resolve dbutils outside a notebook (e.g. in a .py job)."""
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        try:
            from pyspark.dbutils import DBUtils
            return DBUtils(spark)
        except Exception as e:
            raise RuntimeError(f"dbutils unavailable: {e}")

    def get(self, scope: str, key: str) -> str:
        """Single secret value (redacted in logs by the platform)."""
        return self._dbutils.secrets.get(scope=scope, key=key)

    def get_many(self, scope: str, keys: List[str]) -> Dict[str, str]:
        return {k: self.get(scope, k) for k in keys}
