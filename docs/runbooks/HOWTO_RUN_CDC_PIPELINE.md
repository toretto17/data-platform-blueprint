# 🔀 How to: Run a CDC/CDF Pipeline

## Databricks (Delta Change Data Feed)

### Step 1: Enable CDF on the source table
```sql
ALTER TABLE main.bronze.customers SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

### Step 2: Copy the template
```bash
cp databricks/src/de_patterns/cdc_load.py my_cdc_customers.py
```

### Step 3: Configure
```python
class MyCDC(CDCLoadDatabricks):
    KEYS = ["customer_id"]                           # your PK
    SOURCE_TABLE = "main.bronze.customers"           # CDF-enabled source
    TARGET_TABLE = "main.silver.customers"           # table you maintain
```

### Step 4: Run (batch mode)
```python
job = MyCDC()
new_watermark = job.run_batch(last_processed_version=None)  # first run: None
# Save new_watermark somewhere (DDB, Delta table, S3 marker)
```

### Step 5: Run (streaming mode — recommended for production)
```python
job = MyCDC()
job.run_streaming(checkpoint_path="s3://my-bucket/_checkpoints/customers_cdc")
# Checkpoint auto-tracks version — no manual watermark needed
```

## AWS (DMS CDC files + Glue)

### Step 1: Set up DMS task (replicates source → S3 as change files with `Op` column)

### Step 2: Copy template
```bash
cp aws/src/de_patterns/cdc_load.py my_cdc_customers.py
```

### Step 3: Configure
```python
class MyCDC(CDCLoadAWS):
    KEYS = ["customer_id"]
    SOURCE_TYPE = "dms"                               # or "cdf" for Delta sources
    SOURCE_PATH = "s3://my-bucket/cdc/dms/customers/"
    TARGET_TABLE = "silver_db.customers"
    TARGET_PATH = "s3://my-bucket/silver/customers/"
```

### Step 4: Run (Glue job with bookmarks tracks new files automatically)
```bash
aws glue start-job-run --job-name my-cdc-job --arguments '{"--job-bookmark-option":"job-bookmark-enable"}'
```

## How CDC works inside
1. Reads change rows (insert/update/delete)
2. Collapses to NET LATEST change per key (drops duplicates)
3. MERGE into target: upserts inserts/updates, deletes deletes
4. Idempotent — safe to re-run
