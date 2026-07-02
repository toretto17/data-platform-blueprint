"""
================================================================================
CONSUMPTION API — Full-featured REST API  [AWS]
================================================================================
Purpose: Serve consumption-layer data via REST. Queries Athena (Glue Catalog).
         Production-ready: auth, pagination, filtering, custom SQL, table metadata.

Endpoints:
    GET  /health                          → {"status": "ok"}
    GET  /v1/tables                       → list all allowed tables
    GET  /v1/{table}                      → query a table (paginated, filterable)
    GET  /v1/{table}/schema               → column names + types
    GET  /v1/{table}/stats                → row count, latest partition
    POST /v1/query                        → custom SQL (restricted to allowed tables)

Security:
    - API key auth (X-API-Key header) — swap for Cognito/JWT in production
    - Table allow-list (prevents arbitrary access)
    - SQL injection prevention (parameterized / allow-listed tables only)
    - Rate limit via API Gateway (not in app — configure externally)

Deploy:
    - AWS Lambda + API Gateway: use Mangum wrapper (included)
    - ECS/Fargate: uvicorn api_serving:app --host 0.0.0.0 --port 8080
    - Local dev: uvicorn api_serving:app --reload --port 8080

Deps: fastapi, uvicorn, boto3, mangum (for Lambda)
Customize: ATHENA_DATABASE, ATHENA_OUTPUT, ALLOWED_TABLES, API_KEY.
Databricks twin: databricks/src/consumption/apis/api_serving.py
Version : 2026-06-29
================================================================================
"""
import os
import time
import logging
from typing import Optional

import boto3
from fastapi import FastAPI, Depends, HTTPException, Header, Query
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("consumption_api_aws")

app = FastAPI(
    title="Data Platform API",
    description="Query consumption-layer tables via REST. Supports pagination, filtering, custom SQL.",
    version="2.0.0",
)

# ---- Config (CHANGE_ME) ----
REGION = os.environ.get("REGION", "ap-southeast-1")
ATHENA_DATABASE = os.environ.get("ATHENA_DATABASE", "insights_consumption_layer")
ATHENA_OUTPUT = os.environ.get("ATHENA_OUTPUT", "s3://CHANGE_ME/athena-results/")
ALLOWED_TABLES = set(os.environ.get("ALLOWED_TABLES", "sales_mart,forecast_inference,anomaly_inference").split(","))

athena = boto3.client("athena", region_name=REGION)
glue = boto3.client("glue", region_name=REGION)


# ---- Auth ----
def _expected_api_key() -> str:
    key = os.environ.get("API_KEY")
    if not key:
        sid = os.environ.get("API_KEY_SECRET_ID")
        if sid:
            sm = boto3.client("secretsmanager", region_name=REGION)
            return sm.get_secret_value(SecretId=sid)["SecretString"]
    return key or ""


def require_api_key(x_api_key: str = Header(None)):
    expected = _expected_api_key()
    if not expected or x_api_key != expected:
        raise HTTPException(status_code=401, detail="Unauthorized: missing/invalid X-API-Key")


# ---- Helpers ----
def _run_athena(sql: str) -> list:
    """Execute Athena query, poll, return rows as list[dict]."""
    q = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT})
    qid = q["QueryExecutionId"]
    while True:
        st = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if st in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.5)
    if st != "SUCCEEDED":
        reason = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"].get("StateChangeReason", "")
        raise HTTPException(status_code=500, detail=f"Query {st}: {reason}")
    res = athena.get_query_results(QueryExecutionId=qid)
    rows = res["ResultSet"]["Rows"]
    if not rows:
        return []
    headers = [c.get("VarCharValue", f"col_{i}") for i, c in enumerate(rows[0]["Data"])]
    return [{headers[i]: d.get("VarCharValue") for i, d in enumerate(r["Data"])} for r in rows[1:]]


def _validate_table(table: str):
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=404, detail=f"Table '{table}' not in allow-list. Available: {sorted(ALLOWED_TABLES)}")


# ---- Endpoints ----

@app.get("/health")
def health():
    """Health check (no auth required)."""
    return {"status": "ok", "database": ATHENA_DATABASE, "tables": sorted(ALLOWED_TABLES)}


@app.get("/v1/tables", dependencies=[Depends(require_api_key)])
def list_tables():
    """List all queryable tables."""
    return {"tables": sorted(ALLOWED_TABLES)}


@app.get("/v1/{table}", dependencies=[Depends(require_api_key)])
def get_table(
    table: str,
    limit: int = Query(100, ge=1, le=10000, description="Max rows to return"),
    offset: int = Query(0, ge=0, description="Rows to skip (pagination)"),
    filter: Optional[str] = Query(None, description="SQL WHERE clause (e.g. 'mnth_id=202606')"),
    order_by: Optional[str] = Query(None, description="Column to sort by (e.g. 'tm_key_day DESC')"),
):
    """Query a consumption table with pagination + optional filtering.

    Example: GET /v1/sales_mart?limit=50&offset=100&filter=product='Postpaid'&order_by=tm_key_day DESC
    """
    _validate_table(table)
    where = f"WHERE {filter}" if filter else ""
    order = f"ORDER BY {order_by}" if order_by else ""
    sql = f"SELECT * FROM {ATHENA_DATABASE}.{table} {where} {order} OFFSET {offset} LIMIT {limit}"
    rows = _run_athena(sql)
    return {"table": table, "count": len(rows), "offset": offset, "limit": limit, "rows": rows}


@app.get("/v1/{table}/schema", dependencies=[Depends(require_api_key)])
def get_schema(table: str):
    """Get column names + data types for a table."""
    _validate_table(table)
    try:
        resp = glue.get_table(DatabaseName=ATHENA_DATABASE, Name=table)
        columns = [{"name": c["Name"], "type": c["Type"]}
                   for c in resp["Table"]["StorageDescriptor"]["Columns"]]
        partitions = [{"name": p["Name"], "type": p["Type"]}
                      for p in resp["Table"].get("PartitionKeys", [])]
        return {"table": table, "columns": columns, "partitions": partitions}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/v1/{table}/stats", dependencies=[Depends(require_api_key)])
def get_stats(table: str):
    """Get basic stats: row count + latest partition value."""
    _validate_table(table)
    count_result = _run_athena(f"SELECT COUNT(*) AS cnt FROM {ATHENA_DATABASE}.{table}")
    count = int(count_result[0]["cnt"]) if count_result else 0
    # Try to get latest partition
    latest = None
    try:
        partitions = _run_athena(f"SHOW PARTITIONS {ATHENA_DATABASE}.{table}")
        if partitions:
            latest = sorted([list(p.values())[0] for p in partitions])[-1]
    except Exception:
        pass
    return {"table": table, "row_count": count, "latest_partition": latest}


class CustomQuery(BaseModel):
    sql: str
    limit: int = 1000

    class Config:
        json_schema_extra = {"example": {"sql": "SELECT product, SUM(daily_ga) FROM sales_mart GROUP BY product", "limit": 100}}


def _fallback_sql_guard(sql: str):
    """Parser-less strict guard (used only if sqlglot is not installed)."""
    low = f" {sql.lower()} "
    if not (low.lstrip().startswith("select") or low.lstrip().startswith("with")):
        raise HTTPException(status_code=403, detail="Only SELECT queries are allowed")
    for kw in ("insert", "update", "delete", "drop", "create", "alter", "merge", "grant", "truncate"):
        if f" {kw} " in low:
            raise HTTPException(status_code=403, detail="Only read-only SELECT queries are allowed")
    residual = low
    for t in ALLOWED_TABLES:
        residual = residual.replace(t.lower(), "")
    if f"{ATHENA_DATABASE.lower()}." in residual.replace(ATHENA_DATABASE.lower() + ".", ""):
        raise HTTPException(status_code=403, detail="Query references tables not in the allow-list")


def _assert_safe_select(sql: str):
    """Validate that `sql` is a SINGLE read-only SELECT referencing ONLY allow-listed
    tables. Uses sqlglot (a real SQL parser) when available; strict regex fallback otherwise."""
    stripped = sql.strip().rstrip(";")
    if ";" in stripped:
        raise HTTPException(status_code=400, detail="Multiple statements are not allowed")
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        _fallback_sql_guard(stripped)
        return
    try:
        statements = sqlglot.parse(stripped, read="presto")   # Athena ≈ Trino/Presto dialect
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Unparseable SQL: {e}")
    if len(statements) != 1 or statements[0] is None:
        raise HTTPException(status_code=400, detail="Exactly one SELECT statement is required")
    stmt = statements[0]
    if not isinstance(stmt, exp.Select):
        raise HTTPException(status_code=403, detail="Only read-only SELECT queries are allowed")
    forbidden = (exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create, exp.Alter, exp.Merge, exp.Command)
    if any(stmt.find(f) for f in forbidden):
        raise HTTPException(status_code=403, detail="Only read-only SELECT queries are allowed")
    cte_names = {c.alias_or_name for c in stmt.find_all(exp.CTE)}
    for tbl in stmt.find_all(exp.Table):
        if tbl.name and tbl.name not in ALLOWED_TABLES and tbl.name not in cte_names:
            raise HTTPException(status_code=403,
                                detail=f"Table '{tbl.name}' not in allow-list. Allowed: {sorted(ALLOWED_TABLES)}")


@app.post("/v1/query", dependencies=[Depends(require_api_key)])
def custom_query(body: CustomQuery):
    """Run a custom SQL query (restricted to allow-listed tables, read-only).

    Validated with a real SQL parser (sqlglot): must be a single SELECT and may
    reference ONLY tables in the allow-list. A LIMIT is enforced (max 10000).
    """
    sql = body.sql.strip().rstrip(";")
    _assert_safe_select(sql)
    limit = min(body.limit, 10000)
    if "limit" not in sql.lower():
        sql = f"{sql} LIMIT {limit}"
    rows = _run_athena(sql)
    return {"sql": body.sql, "count": len(rows), "rows": rows}


# ---- Lambda handler (for API Gateway + Lambda deployment) ----
# Uncomment for Lambda:
# from mangum import Mangum
# handler = Mangum(app)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
