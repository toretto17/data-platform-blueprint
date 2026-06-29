# Redshift Spectrum → Local Table: How Data Flows from S3 to Redshift

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         S3 (Source of Truth)                              │
│  s3://bucket-consumption-accountid/sales_mart/mnth_id=202606/            │
│  (Parquet files written by Glue consumption job)                         │
└──────────────────────────────────┬───────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    AWS Glue Data Catalog                                  │
│  Database: insights_consumption_layer                                    │
│  Table: sales_mart (Parquet, partitioned by mnth_id)                     │
│  Location: s3://bucket-consumption-accountid/sales_mart/                  │
└──────────────────────────────────┬───────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│              Redshift Spectrum (External Schema)                          │
│  Schema: insights_consumption_layer_spectrum                              │
│  → Maps to Glue DB: insights_consumption_layer                           │
│  → Reads S3 data DIRECTLY (no copy, no storage in Redshift)             │
│  → Uses IAM Role: role-bnic-aii-nonprod-redshift                         │
│                                                                          │
│  Tables here are VIRTUAL — they read S3 parquet on-the-fly              │
└──────────────────────────────────┬───────────────────────────────────────┘
                                   │
                                   ▼ (INSERT INTO ... SELECT FROM spectrum)
┌─────────────────────────────────────────────────────────────────────────┐
│              Redshift Local Table (Materialized Copy)                     │
│  Schema: insights_consumption_layer                                      │
│  Table: sales_mart                                                        │
│  → ACTUAL Redshift storage (columnar, compressed, fast queries)          │
│  → Refreshed by stored procedure on each ETL run                         │
│  → This is what BI dashboards / chatbot queries                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## How It Works (Step by Step)

### 1. Glue writes Parquet to S3
```
Glue consumption job → writes to:
  s3://s3-bnic-aii-nonprod-consumption-503561443692/sales_mart/mnth_id=202606/part-00000.parquet
```

### 2. Glue Catalog already has the table registered
The Glue consumption job uses `saveAsTable()` or `MSCK REPAIR TABLE` which registers partitions in Glue Catalog.

### 3. Spectrum external schema reads Glue Catalog
```sql
-- This was created ONCE (setup):
CREATE EXTERNAL SCHEMA insights_consumption_layer_spectrum
FROM DATA CATALOG
DATABASE 'insights_consumption_layer'    -- Glue Catalog database name
IAM_ROLE 'arn:aws:iam::ACCOUNT:role/role-bnic-aii-ENV-redshift'
REGION 'ap-southeast-1';
```
After this, any table in the Glue DB is automatically visible as a Spectrum table. **No per-table setup needed.**

### 4. Stored Procedure copies Spectrum → Local
The framework calls `sp__latest_snapshot_data_update` which:
1. Looks up `schema_dataload_fw.latest_snapshot_load_table_mapping` for the table
2. Runs `DELETE FROM insights_consumption_layer.sales_mart` (clear local)
3. Runs `INSERT INTO insights_consumption_layer.sales_mart SELECT * FROM insights_consumption_layer_spectrum.sales_mart` (copy from S3)
4. Records row count and timestamp

### 5. BI tools query the LOCAL table
```sql
-- Fast query (data is IN Redshift, columnar, compressed):
SELECT * FROM insights_consumption_layer.sales_mart WHERE mnth_id = 202606;
```

## The Mapping Table

`schema_dataload_fw.latest_snapshot_load_table_mapping` controls everything:

| Column | Purpose |
|---|---|
| `source_db` | Always `bnic_aii_db` |
| `source_schema` | Spectrum schema (e.g., `insights_consumption_layer_spectrum`) |
| `source_table` | Source table in Spectrum |
| `target_db` | Always `bnic_aii_db` |
| `target_schema` | Local schema (e.g., `insights_consumption_layer`) |
| `target_table` | Local table name |
| `latest_data_upd_script_1..9` | The INSERT INTO ... SELECT FROM ... SQL (split across columns) |
| `latest_data_upd_rec_cnt` | Row count after last load |
| `latest_data_upd_datetime` | Timestamp of last load |

## How to Register a New Table

### Step 1: Create the local Redshift table
```sql
CREATE TABLE IF NOT EXISTS insights_consumption_layer.my_new_table (
    site_code VARCHAR(20),
    data_dt INTEGER,
    mnth_id INTEGER,
    daily_metric NUMERIC(18,2)
    -- ... all columns matching the Parquet schema
) DISTSTYLE AUTO SORTKEY AUTO ENCODE AUTO;
```

### Step 2: Insert mapping row
```sql
INSERT INTO schema_dataload_fw.latest_snapshot_load_table_mapping (
    source_db, source_schema, source_table,
    target_db, target_schema, target_table,
    etl_group,
    latest_data_upd_script_1,
    latest_data_upd_script_5,
    latest_data_upd_script_9
) VALUES (
    'bnic_aii_db', 'insights_consumption_layer_spectrum', 'my_new_table',
    'bnic_aii_db', 'insights_consumption_layer', 'my_new_table',
    'mobile_analytics',
    'INSERT INTO insights_consumption_layer.my_new_table (',
    ') SELECT ',
    ' FROM insights_consumption_layer_spectrum.my_new_table ;'
);
-- Note: scripts 2-4 and 6-8 contain the column lists (split due to VARCHAR limits)
```

### Step 3: Add to DDB config (so framework SF triggers the load)
```json
"redshift_config": {
    "M": {
        "target_tables_list": {
            "L": [{
                "M": {
                    "target_database": {"S": "bnic_aii_db"},
                    "target_schema": {"S": "insights_consumption_layer"},
                    "target_table": {"S": "my_new_table"}
                }
            }]
        }
    }
}
```

### Step 4: The SF calls the stored procedure
When the consumption pipeline runs:
```
Framework SF → step 6b_start_redshift_load → Redshift Dataload SF
  → CALL schema_dataload_fw.sp__latest_snapshot_data_update('bnic_aii_db','insights_consumption_layer','my_new_table','NULL');
  → DELETE + INSERT INTO ... SELECT FROM ... spectrum
```

## Why This Pattern?

| Approach | Speed | Cost | When to Use |
|---|---|---|---|
| Query Spectrum directly | Slow (S3 scan) | Pay per scan | Ad-hoc queries, exploration |
| Local table (this pattern) | Fast (columnar) | Storage cost | BI dashboards, chatbot, repeated queries |
| COPY from S3 | Fast | Storage cost | Alternative to Spectrum for non-catalog data |

**This project uses the Spectrum → Local pattern because:**
- BI dashboards need sub-second queries (Spectrum is 5-30s for large tables)
- Data freshness is daily (local table refreshed each ETL run)
- Schema defined once in mapping table (no manual COPY commands)

## Key Configuration

### External Schema Creation (one-time setup per environment)
```sql
CREATE EXTERNAL SCHEMA insights_consumption_layer_spectrum
FROM DATA CATALOG
DATABASE 'insights_consumption_layer'
IAM_ROLE 'arn:aws:iam::${account_id}:role/role-${project}-${feature}-${environment}-redshift'
REGION '${region}';
```

### IAM Role Requirements
The Redshift IAM role needs:
- `s3:GetObject` on the S3 consumption bucket
- `glue:GetTable`, `glue:GetPartitions` on the Glue Catalog database
- `sts:AssumeRole` (if cross-account)

### Stored Procedure Logic
```
sp__latest_snapshot_data_update(database, schema, table, overwrite_condition):
  1. Look up mapping table for source → target SQL
  2. DELETE FROM target (full replace)
  3. Execute: INSERT INTO target SELECT * FROM spectrum_source
  4. Record row count + timestamp in mapping table
```

## Monitoring

```sql
-- Check last load status for all tables:
SELECT target_table, latest_data_upd_rec_cnt, latest_data_upd_datetime, latest_data_upd_duration
FROM schema_dataload_fw.latest_snapshot_load_table_mapping
WHERE target_schema = 'insights_consumption_layer'
ORDER BY latest_data_upd_datetime DESC;
```
