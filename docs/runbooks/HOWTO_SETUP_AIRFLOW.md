# 🌬️ How to: Set Up an Airflow DAG

## When to use Airflow (vs Step Functions / Databricks Workflows)
- You already have Airflow (MWAA / Cloud Composer / self-hosted)
- You need cross-cloud orchestration (AWS + GCP + on-prem)
- Your team prefers Python-defined DAGs over JSON/YAML

## Step 1: Copy the template
```bash
cp aws/src/orchestration/airflow_dag_template.py dags/my_etl_pipeline.py
```

## Step 2: Configure the DAG
```python
with DAG(dag_id="sales-etl-pipeline", schedule_interval="0 18 * * *", ...):
    silver = GlueJobOperator(task_id="silver", job_name="glue_bnic_aii_silver_sales")
    gold = GlueJobOperator(task_id="gold", job_name="glue_bnic_aii_gold_sales_mart")
    silver >> gold
```

## Step 3: Deploy to Airflow
### MWAA (AWS managed)
```bash
aws s3 cp dags/my_etl_pipeline.py s3://my-mwaa-bucket/dags/
```

### Self-hosted
```bash
cp dags/my_etl_pipeline.py $AIRFLOW_HOME/dags/
```

## Step 4: Verify
Go to Airflow UI → DAGs → find your DAG → toggle ON → Trigger

## Operators available
| Operator | Platform | What it does |
|---|---|---|
| `GlueJobOperator` | AWS | Triggers a Glue job + waits |
| `DatabricksSubmitRunOperator` | Databricks | Submits a notebook/job |
| `S3KeySensor` | AWS | Waits for a file to appear in S3 |
| `PythonOperator` | Any | Runs any Python function |
