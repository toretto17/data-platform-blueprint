"""
================================================================================
CONSUMPTION API — REST serving over the consumption layer  [AWS]
================================================================================
Purpose: Expose curated consumption data as a REST API for apps/services.
         Queries Athena (consumption Glue tables) and returns JSON.

⚠️ SECURITY: this template ships with API-key auth ENABLED by default. NEVER
deploy a data API without authentication. For production prefer a managed
authorizer (API Gateway + Cognito/IAM/JWT) in front of this app.

Deploy options:
    • AWS Lambda + API Gateway (use Mangum to wrap the FastAPI app), OR
    • a container on ECS/Fargate.

Customize (CHANGE_ME):
    - ATHENA_DATABASE, ATHENA_OUTPUT (S3 results), allowed tables/queries
    - API key storage (here: Secrets Manager / env); swap for Cognito/JWT in prod

Run locally:  uvicorn api_serving:app --port 8080
Deps:        fastapi, uvicorn, boto3, mangum (for Lambda)
Version : 2026-06-28
================================================================================
"""
import os
import logging

import boto3
from fastapi import FastAPI, Depends, HTTPException, Header, Query

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("consumption_api_aws")

app = FastAPI(title="Consumption API", version="1.0.0")

# ---- config (CHANGE_ME) ----
REGION = os.environ.get("REGION", "ap-southeast-1")
ATHENA_DATABASE = os.environ.get("ATHENA_DATABASE", "insights_consumption_layer")
ATHENA_OUTPUT = os.environ.get("ATHENA_OUTPUT", "s3://CHANGE_ME/athena-results/")
# Allow-list the tables the API may read (prevents arbitrary table access / injection).
ALLOWED_TABLES = set(os.environ.get("ALLOWED_TABLES", "sales_mart").split(","))

athena = boto3.client("athena", region_name=REGION)


# ---- authentication (API key) ----
def _expected_api_key() -> str:
    """Fetch the expected API key. CHANGE_ME for prod (Secrets Manager / Cognito/JWT)."""
    key = os.environ.get("API_KEY")
    if not key:
        # fall back to Secrets Manager if configured
        sid = os.environ.get("API_KEY_SECRET_ID")
        if sid:
            sm = boto3.client("secretsmanager", region_name=REGION)
            return sm.get_secret_value(SecretId=sid)["SecretString"]
    return key or ""


def require_api_key(x_api_key: str = Header(None)):
    expected = _expected_api_key()
    if not expected or x_api_key != expected:
        raise HTTPException(status_code=401, detail="Unauthorized: missing/invalid X-API-Key")


# ---- helpers ----
def _run_athena(sql: str) -> list[dict]:
    """Run a query and return rows as list[dict]. Polls to completion."""
    import time
    q = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT})
    qid = q["QueryExecutionId"]
    while True:
        st = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if st in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if st != "SUCCEEDED":
        raise HTTPException(status_code=500, detail=f"Query {st}")
    res = athena.get_query_results(QueryExecutionId=qid)
    rows = res["ResultSet"]["Rows"]
    if not rows:
        return []
    headers = [c["VarCharValue"] for c in rows[0]["Data"]]
    return [{headers[i]: d.get("VarCharValue") for i, d in enumerate(r["Data"])} for r in rows[1:]]


# ---- endpoints ----
@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/v1/{table}", dependencies=[Depends(require_api_key)])
def get_table(table: str, limit: int = Query(100, le=1000)):
    """Return up to `limit` rows from an allow-listed consumption table."""
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=404, detail=f"Unknown/again-not-allowed table: {table}")
    # Parameterize safely: table is allow-listed; limit is bounded int.
    sql = f"SELECT * FROM {ATHENA_DATABASE}.{table} LIMIT {int(limit)}"
    return {"table": table, "rows": _run_athena(sql)}


# For AWS Lambda deployment, wrap with Mangum:
#   from mangum import Mangum
#   handler = Mangum(app)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
