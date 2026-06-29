<div align="center">

# 🏛️ End-to-End Architecture

*How data flows through the platform — from raw sources to ML predictions*

</div>

---

## 🗺️ High-Level Data Flow

```mermaid
graph TB
    %% Styling
    classDef source fill:#e1f5fe,stroke:#0288d1,color:#01579b
    classDef bronze fill:#fff3e0,stroke:#f57c00,color:#e65100
    classDef silver fill:#e8f5e9,stroke:#388e3c,color:#1b5e20
    classDef gold fill:#fff9c4,stroke:#f9a825,color:#f57f17
    classDef consumption fill:#f3e5f5,stroke:#7b1fa2,color:#4a148c
    classDef ml fill:#fce4ec,stroke:#c62828,color:#b71c1c
    classDef ops fill:#eceff1,stroke:#455a64,color:#263238

    %% Sources
    S3[☁️ S3 Files]:::source
    JDBC[🗄️ Databases JDBC]:::source
    KAFKA[📡 Kafka / Kinesis]:::source

    %% Bronze
    INGEST[📥 Ingestion<br/><i>batch + streaming</i>]:::bronze
    BRONZE[🥉 Bronze<br/><i>Raw + Lineage<br/>Append-only</i>]:::bronze

    %% Silver
    SILVER[🥈 Silver<br/><i>Cleansed + Deduped<br/>Typed + DQ validated</i>]:::silver

    %% Gold
    GOLD[🥇 Gold<br/><i>Business Aggregates<br/>MTD / YTD / Windows</i>]:::gold

    %% Feature Store + ML
    FE[⚙️ Feature Engineering<br/><i>Lags, Rolling, Calendar</i>]:::ml
    FS[📦 Feature Store<br/><i>PIT Joins + Lineage</i>]:::ml
    TRAIN[🏋️ Training<br/><i>Optuna HPO + Eval Gate</i>]:::ml
    REG[📋 Model Registry<br/><i>Champion / Challenger</i>]:::ml
    INF[🎯 Inference<br/><i>Batch + Realtime</i>]:::ml
    MON[👁️ Monitoring<br/><i>PSI / KS Drift</i>]:::ml

    %% Consumption
    REDSHIFT[📊 Redshift / DBSQL]:::consumption
    SNOWFLAKE[❄️ Snowflake]:::consumption
    API[🌐 REST API]:::consumption
    BI[📈 BI Dashboards]:::consumption

    %% Ops
    ORCH[🔄 Orchestration<br/><i>SF / Workflows / Airflow</i>]:::ops
    CICD[🚀 CI/CD<br/><i>GitHub Actions / DAB</i>]:::ops

    %% Flow
    S3 & JDBC & KAFKA --> INGEST --> BRONZE --> SILVER --> GOLD

    GOLD --> REDSHIFT & SNOWFLAKE & API --> BI
    GOLD --> FE --> FS --> TRAIN --> REG --> INF
    INF --> GOLD
    MON -.->|drift alert| TRAIN
    INF -.-> MON

    ORCH -.->|schedules| INGEST & SILVER & GOLD & TRAIN & INF
    CICD -.->|deploys| ORCH
```

---

## 🔀 Load Patterns (Decision Matrix)

```mermaid
graph TD
    classDef pattern fill:#e8eaf6,stroke:#3f51b5,color:#1a237e
    classDef decision fill:#fff8e1,stroke:#ff8f00,color:#e65100

    START[🤔 How does your data arrive?]:::decision
    CDC_Q{Row-level changes<br/>inserts/updates/deletes?}:::decision
    HIST_Q{Need dimension history?}:::decision
    WM_Q{Have a watermark column?}:::decision

    CDC[🔀 CDC / CDF Load<br/><code>de_patterns/cdc_load.py</code>]:::pattern
    SCD2[📚 SCD Type 2<br/><code>de_patterns/scd_type2.py</code>]:::pattern
    SCD1[📝 SCD Type 1<br/><code>de_patterns/scd_type1.py</code>]:::pattern
    INCR[🔄 Incremental Load<br/><code>de_patterns/incremental_load.py</code>]:::pattern
    FULL[📦 Full Load<br/><code>de_patterns/full_load.py</code>]:::pattern

    START --> CDC_Q
    CDC_Q -->|Yes| CDC
    CDC_Q -->|No| HIST_Q
    HIST_Q -->|Yes, track all versions| SCD2
    HIST_Q -->|No, just current| SCD1
    SCD1 --> WM_Q
    HIST_Q -->|Not a dimension| WM_Q
    WM_Q -->|Yes| INCR
    WM_Q -->|No / Small table| FULL
```

---

## 🤖 MLOps Lifecycle

```mermaid
graph LR
    classDef stage fill:#e8f5e9,stroke:#2e7d32,color:#1b5e20
    classDef gate fill:#ffebee,stroke:#c62828,color:#b71c1c
    classDef serve fill:#e3f2fd,stroke:#1565c0,color:#0d47a1

    FE[⚙️ Features<br/><i>Feature Store</i>]:::stage
    TRAIN[🏋️ Train<br/><i>Optuna + CV</i>]:::stage
    EVAL[📊 Evaluate<br/><i>F1 / AUC / RMSE</i>]:::gate
    REG[📋 Register<br/><i>UC / Model Registry</i>]:::stage
    DEPLOY[🚀 Deploy<br/><i>Canary → Promote</i>]:::serve
    SERVE[🎯 Serve<br/><i>Batch + Realtime</i>]:::serve
    MONITOR[👁️ Monitor<br/><i>Drift Detection</i>]:::gate

    FE --> TRAIN --> EVAL
    EVAL -->|✅ Pass| REG --> DEPLOY --> SERVE --> MONITOR
    EVAL -->|❌ Fail| TRAIN
    MONITOR -->|🚨 Drift| TRAIN
```

---

## 🔗 How Components Connect

| From | To | How |
|:---|:---|:---|
| **Gold** → Feature Store | `fe.write_table` / `FeatureStoreManager.ingest_data` | Scheduled job computes + writes features |
| **Feature Store** → Training | `fe.create_training_set` + `FeatureLookup` | Auto PIT-join on timestamp columns |
| **Training** → Registry | `fe.log_model` (Databricks) / `create_model_package` (AWS) | Packages feature lineage INTO the model |
| **Registry** → Inference | `fe.score_batch(model_uri)` / Batch Transform from latest Approved | Auto-fetches features at inference time |
| **Inference** → Consumption | Output predictions → Gold table → consumption ETL | Merged with actuals for BI |
| **Monitoring** → Training | Drift alert (PSI > 0.2) → triggers retrain pipeline | Closes the feedback loop |

---

## 🏗️ Platform-Specific Implementation

<table>
<tr>
<th width="50%">☁️ AWS (Glue + SageMaker)</th>
<th width="50%">🧱 Databricks (Delta + UC + MLflow)</th>
</tr>
<tr>
<td>

```
EventBridge (cron)
    → Step Functions (DAG)
        → Glue Jobs (Spark ETL)
            → S3 (Parquet/Delta)
                → Glue Catalog
                    → Athena / Redshift

SageMaker Pipelines (@step)
    → Training → Eval → Register
        → Batch Transform
            → Model Monitor
```

</td>
<td>

```
Databricks Workflows (cron)
    → Notebook Tasks (Spark ETL)
        → Delta Tables
            → Unity Catalog
                → DBSQL / Snowflake

MLflow + FeatureEngineeringClient
    → Training → Eval → Register (UC)
        → Model Serving (scale-to-zero)
            → Lakehouse Monitor
```

</td>
</tr>
</table>

---

## 📐 Best Practices Embedded in Architecture

| Layer | Practice | Why |
|:---|:---|:---|
| Bronze | Append-only + lineage | Never lose raw data; full audit trail |
| Silver | Dedup + DQ before downstream | Garbage in ≠ garbage out |
| Gold | Dynamic partition overwrite | Idempotent; safe to re-run |
| Feature Store | PIT joins (no future leakage) | ML correctness |
| Training | Eval gate before registration | Bad models never reach production |
| Deployment | Canary first, rollback ready | Minimize blast radius |
| Monitoring | PSI/KS drift detection | Catch degradation before users do |
| All layers | Early-exit (skip if fresh) | Save compute $$ on no-op runs |
