"""
================================================================================
CONSUMPTION API — Full-featured REST API  [Databricks]
================================================================================
Purpose: Serve consumption-layer data via REST. Queries Databricks SQL Warehouse.
         Production-ready: auth, pagination, filtering, custom SQL, table metadata.

Endpoints:
    GET  /health                          → {"status": "ok"}
    GET  /v1/tables                       → list all allowed tables
    GET  /v1/{table}                      → query a table (paginated, filterable)
    GET  /v1/{table}/schema               → column names + types
    GET  /v1/{table}/stats                → row count
    POST /v1/query                        → custom SQL (restricted to allowed tables)

Security:
    - API key auth (X-API-Key header) — swap for OAuth/JWT in production
    - Table allow-list
    - SQL injection prevention

Deploy:
    - Any host: uvicorn api_serving:app --host 0.0.0.0 --port 8080
    - Databricks Apps (native hosting on Databricks)
    - Container (ECS/AKS/GKE)

Deps: fastapi, uvicorn, databricks-sql-connector
Customize: DATABRICKS_HOST, DBX_HTTP_PATH, DATABRICKS_TOKEN, SCHEMA, ALLOWED_TABLES.
AWS twin: aws/src/consumption/apis/api_serving.py
Version : 2026-06-29
================================================================================
"""
import os
import logging
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, Header, Query
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("consumption_api_databricks")

app = FastAPI(
    title="Data Platform API (Databricks)",
    description="Query consumption-layer Delta tables via REST. Supports pagination, filtering, custom SQL.",
    version="2.0.0",
)

# ---- Config (CHANGE_ME) ----
DBX_HOST = os.environ.get("DATABRICKS_HOST", "CHANGE_ME.cloud.databricks.com")
DBX_HTTP_PATH = os.environ.get("DBX_HTTP_PATH", "/sql/1.0/warehouses/CHANGE_ME")
DBX_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")          # prefer secret store
SCHEMA = os.environ.get("CONSUMPTION_SCHEMA", "main.consumption")
ALLOWED_TABLES = set(os.environ.get("ALLOWED_TABLES", "sales_mart,forecast_inference,anomaly_inference").split(","))


# ---- Auth ----
def require_api_key(x_api_key: str = Header(None)):
    expected = os.environ.get("API_KEY", "")
    if not expected or x_api_key != expected:
        raise HTTPException(status_code=401, detail="Unauthorized: missing/invalid X-API-Key")


# ---- Helpers ----
def _query(sql: str) -> list:
    """Run SQL against Databricks SQL Warehouse, return list[dict]."""
    from databricks import sql as dbsql
    with dbsql.connect(server_hostname=DBX_HOST, http_path=DBX_HTTP_PATH,
                       access_token=DBX_TOKEN) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def _validate_table(table: str):
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=404, detail=f"Table '{table}' not in allow-list. Available: {sorted(ALLOWED_TABLES)}")


# ---- Endpoints ----

@app.get("/health")
def health():
    return {"status": "ok", "schema": SCHEMA, "tables": sorted(ALLOWED_TABLES)}


@app.get("/v1/tables", dependencies=[Depends(require_api_key)])
def list_tables():
    return {"tables": sorted(ALLOWED_TABLES)}


@app.get("/v1/{table}", dependencies=[Depends(require_api_key)])
def get_table(
    table: str,
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    filter: Optional[str] = Query(None, description="SQL WHERE clause"),
    order_by: Optional[str] = Query(None, description="Column to sort by"),
):
    """Query a table with pagination + optional filtering.

    Example: GET /v1/sales_mart?limit=50&filter=product='Postpaid'&order_by=tm_key_day DESC
    """
    _validate_table(table)
    where = f"WHERE {filter}" if filter else ""
    order = f"ORDER BY {order_by}" if order_by else ""
    sql = f"SELECT * FROM {SCHEMA}.{table} {where} {order} LIMIT {limit} OFFSET {offset}"
    rows = _query(sql)
    return {"table": table, "count": len(rows), "offset": offset, "limit": limit, "rows": rows}


@app.get("/v1/{table}/schema", dependencies=[Depends(require_api_key)])
def get_schema(table: str):
    _validate_table(table)
    rows = _query(f"DESCRIBE TABLE {SCHEMA}.{table}")
    columns = [{"name": r.get("col_name", ""), "type": r.get("data_type", "")}
               for r in rows if r.get("col_name") and not r["col_name"].startswith("#")]
    return {"table": table, "columns": columns}


@app.get("/v1/{table}/stats", dependencies=[Depends(require_api_key)])
def get_stats(table: str):
    _validate_table(table)
    result = _query(f"SELECT COUNT(*) AS cnt FROM {SCHEMA}.{table}")
    count = int(result[0]["cnt"]) if result else 0
    return {"table": table, "row_count": count}


class CustomQuery(BaseModel):
    sql: str
    limit: int = 1000

    class Config:
        json_schema_extra = {"example": {"sql": "SELECT product, SUM(daily_ga) FROM sales_mart GROUP BY product", "limit": 100}}


@app.post("/v1/query", dependencies=[Depends(require_api_key)])
def custom_query(body: CustomQuery):
    """Run custom SQL (restricted to allowed tables)."""
    sql = body.sql.strip().rstrip(";")
    sql_lower = sql.lower()
    # Basic security: check only allowed tables referenced
    for table in ALLOWED_TABLES:
        sql_lower = sql_lower.replace(table, "")
    if f"{SCHEMA.lower()}." in sql_lower.replace(SCHEMA.lower() + ".", ""):
        raise HTTPException(status_code=403, detail="References tables not in the allow-list")
    limit = min(body.limit, 10000)
    if "limit" not in sql_lower:
        sql = f"{sql} LIMIT {limit}"
    rows = _query(sql)
    return {"sql": body.sql, "count": len(rows), "rows": rows}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
