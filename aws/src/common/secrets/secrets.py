"""
================================================================================
SECRETS HELPER — [AWS]
================================================================================
Purpose: One place to fetch secrets. Supports AWS Secrets Manager (JSON or plain)
         and SSM Parameter Store (SecureString). Never hardcode credentials.

Usage:
    from aws.src.common.secrets.secrets import Secrets
    sec = Secrets(region="ap-southeast-1")
    creds = sec.get_json("prod/db/credentials")        # → {"username":..., "password":...}
    user = sec.get_json("prod/db/credentials")["username"]
    token = sec.get_string("/myapp/api_token")         # SSM SecureString
Version : 2026-06-28
================================================================================
"""
import json
import logging
from functools import lru_cache

import boto3

logger = logging.getLogger("secrets_aws")


class Secrets:
    def __init__(self, region: str = "ap-southeast-1"):
        self.region = region
        self._sm = boto3.client("secretsmanager", region_name=region)
        self._ssm = boto3.client("ssm", region_name=region)

    @lru_cache(maxsize=128)
    def get_secret_string(self, secret_id: str) -> str:
        """Raw string from Secrets Manager."""
        return self._sm.get_secret_value(SecretId=secret_id)["SecretString"]

    def get_json(self, secret_id: str) -> dict:
        """Parse a JSON secret from Secrets Manager into a dict."""
        return json.loads(self.get_secret_string(secret_id))

    @lru_cache(maxsize=128)
    def get_string(self, parameter_name: str) -> str:
        """SecureString from SSM Parameter Store (auto-decrypted)."""
        return self._ssm.get_parameter(Name=parameter_name, WithDecryption=True)["Parameter"]["Value"]
