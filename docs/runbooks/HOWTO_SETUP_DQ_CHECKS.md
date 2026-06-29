# ✅ How to: Set Up Data Quality Checks

## Step 1: Import the framework
```python
# AWS
from aws.src.common.validations.dq_framework import DataQualityFramework, DQConfig, DQCheck, Severity

# Databricks
from databricks.src.common.validations.dq_framework import DataQualityFramework, DQConfig, DQCheck, Severity
```

## Step 2: Define your checks
```python
config = DQConfig(
    table_name="silver_db.sales",
    checks=[
        DQCheck("rows", "row_count", Severity.CRITICAL, {"min_count": 1000}),
        DQCheck("pk_not_null", "completeness", Severity.CRITICAL, {"column": "id", "max_null_pct": 0.0}),
        DQCheck("no_future_dates", "business_rule", Severity.HIGH,
                {"sql": "SELECT * FROM __dq_check_table WHERE data_dt > 20261231"}),
        DQCheck("schema", "schema", Severity.HIGH,
                {"expected_columns": ["id", "amount", "data_dt", "mnth_id"]}),
        DQCheck("fresh", "freshness", Severity.MEDIUM, {"partition_column": "mnth_id"}),
        DQCheck("vs_source", "reconciliation", Severity.HIGH, {"source_count": 50000, "threshold_pct": 0.01}),
    ],
)
```

## Step 3: Run in your job
```python
dq = DataQualityFramework(spark)
report = dq.validate(df, config)

if report.has_failures:
    dq.publish_metrics(report)        # → CloudWatch (AWS) or Delta audit table (DBX)
    raise Exception(report.summary)   # stop pipeline
```

## Available check types
| Type | What it validates | Params |
|---|---|---|
| `row_count` | Min rows | `{"min_count": N}` |
| `completeness` | Null/empty % | `{"column": "col", "max_null_pct": 0.05}` |
| `schema` | Required columns exist | `{"expected_columns": ["a","b"]}` |
| `freshness` | Latest partition not null | `{"partition_column": "mnth_id"}` |
| `business_rule` | Custom SQL returns 0 rows | `{"sql": "SELECT ... WHERE violation"}` |
| `reconciliation` | Count matches source ±threshold | `{"source_count": N, "threshold_pct": 0.01}` |

## Per-layer DQ configs (ready to use)
```python
from aws.src.bronze.dq.bronze_dq import bronze_dq_config
from aws.src.silver.dq.silver_dq import silver_dq_config
from aws.src.gold.dq.gold_dq import gold_dq_config
```
