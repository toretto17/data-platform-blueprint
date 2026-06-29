# How to: Add a new ETL pipeline

## Steps

1. **Create your table schema** (Silver/Gold) in your planning doc
2. **Copy the layer template:**
   - AWS: `aws/src/silver/jobs/silver_job.py` → `aws/src/silver/jobs/silver_<entity>.py`
   - Databricks: same path in `databricks/`
3. **Fill in CHANGE_ME:**
   - `_define_sources()` — point to your source table(s)
   - `_apply_transformations()` — your business logic (cleanse/cast/dedup)
   - `_derive_columns()` — computed columns (mnth_id, data_dt, etc.)
   - `_dq_config()` — DQ checks for this table
4. **Create DDB config** (AWS): copy `configs/templates/ddb_config.json.template`, fill values
5. **Add to orchestration:**
   - AWS: add a step to the master SF (or add in `locals-gluejobs.tf`)
   - Databricks: add a task in `databricks.yml` or `orchestration.py`
6. **Deploy:**
   - `make deploy-glue ENV=dev` (uploads script)
   - `make tf-apply ENV=dev` (creates Glue job + SF)
   - OR `databricks bundle deploy -t dev`
7. **Trigger + verify:**
   - Run the SF or workflow
   - Check DQ report, row counts, partition freshness
8. **Add to monitoring** (optional): set up alerts for failure/drift

## Naming convention
- Job name: `glue_<project>_<feature>_<layer>_<entity>` (AWS)
- Table: `<domain>_<layer>.<layer>_<entity>` (e.g. `analytics_silver.silver_sales`)
