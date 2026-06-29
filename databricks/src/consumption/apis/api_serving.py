"""
================================================================================
CONSUMPTION API — REST serving over the consumption layer  [Databricks]
================================================================================
Purpose: Twin of aws/src/consumption/apis/api_serving.py. Serves curated
         consumption data via REST, querying Databricks SQL (a SQL Warehouse)
         through the databricks-sql-connector.

⚠️ SECURITY: API-key auth ENABLED by default. Never deploy a data API without
auth. In production prefer OAuth/JWT (e.g. behind your API gateway / SSO).

Deploy options:
    • a container (ECS/AKS/GKE) or any Python host, OR
    • Databricks Apps (serve the FastAPI app on Databricks).

Customize (CHANGE_ME):
    - DATABRICKS_HOST, HTTP_PATH (SQL warehouse), token (secret), schema
    - ALLOWED_TABLES allow-list

Run locally:  uvicorn api_serving:app --port 8080
Deps:        fastapi, uvicorn, databricks-sql-connector
Version : 2026-06-28
================================================================================
"""
import os
import logging

from fastapi import FastAPI, Depends, HTTPException, Header, Query

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("consumption_api_databricks")

app = FastAPI(title="Consumption API (Databricks)", version="1.0.0")

# ---- config (CHANGE_ME) ----
DBX_HOST = os.environ.get("DATABRICKS_HOST", "CHANGE_ME.cloud.databricks.com")
DBX_HTTP_PATH = os.environ.get("DBX_HTTP_PATH", "/sql/1.0/warehouses/CHANGE_ME")
DBX_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")          # prefer secret store
SCHEMA = os.environ.get("CONSUMPTION_SCHEMA", "main.consumption")
ALLOWED_TABLES = set(os.environ.get("ALLOWED_TABLES", "sales_mart").split(","))


def require_api_key(x_api_key: str = Header(None)):
    expected = os.environ.get("API_KEY", "")
    if not expected or x_api_key != expected:
        raise HTTPException(status_code=401, detail="Unauthorized: missing/invalid X-API-Key")


def _query(sql: str) -> list[dict]:
    """Run a query against a Databricks SQL Warehouse, return list[dict]."""
    from databricks import sql as dbsql
    with dbsql.connect(server_hostname=DBX_HOST, http_path=DBX_HTTP_PATH,
                       access_token=DBX_TOKEN) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/v1/{table}", dependencies=[Depends(require_api_key)])
def get_table(table: str, limit: int = Query(100, le=1000)):
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=404, detail=f"Unknown/not-allowed table: {table}")
    sql = f"SELECT * FROM {SCHEMA}.{table} LIMIT {int(limit)}"
    return {"table": table, "rows": _query(sql)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
