# ❄️ How to: Connect and Write to Snowflake

## Step 1: Store credentials (NEVER hardcode)

### AWS (Secrets Manager)
```bash
aws secretsmanager create-secret --name prod/snowflake \
  --secret-string '{"url":"abc.snowflakecomputing.com","user":"ETL_USER","password":"..."}'
```

### Databricks (Secret scope)
```bash
databricks secrets create-scope prod-snowflake
databricks secrets put-secret prod-snowflake sfURL --string-value "abc.snowflakecomputing.com"
databricks secrets put-secret prod-snowflake sfUser --string-value "ETL_USER"
databricks secrets put-secret prod-snowflake sfPassword --string-value "..."
```

## Step 2: Install Spark-Snowflake connector

### AWS Glue
Add to job params: `--extra-jars s3://bucket/jars/spark-snowflake_2.12-2.12.0-spark_3.4.jar,s3://bucket/jars/snowflake-jdbc-3.14.4.jar`

### Databricks
Pre-installed on most runtimes. If not: install `net.snowflake:spark-snowflake` library on cluster.

## Step 3: Run the load

### Write modes
```python
# Overwrite (full refresh)
SnowflakeLoad({"mode": "overwrite", "table": "ANALYTICS.PUBLIC.SALES_MART"}).run(df)

# Append (incremental)
SnowflakeLoad({"mode": "append", "table": "ANALYTICS.PUBLIC.EVENTS"}).run(df)

# Merge/Upsert (by key)
SnowflakeLoad({"mode": "merge", "table": "ANALYTICS.PUBLIC.DIM_CUSTOMER",
               "keys": ["customer_id"]}).run(df)
```

## How MERGE works internally
1. Writes df to a temp staging table in Snowflake
2. Runs: `MERGE INTO target USING staging ON keys WHEN MATCHED UPDATE WHEN NOT MATCHED INSERT`
3. Drops the staging table
4. Atomic — target is never in an inconsistent state
