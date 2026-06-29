# 📡 How to: Set Up Streaming Ingestion

## Databricks (Autoloader — recommended for files)

### Step 1: Copy template
```bash
cp databricks/src/ingestion/streaming/stream_ingest.py my_stream.py
```

### Step 2: Configure
```python
cfg = {
    "mode": "autoloader",
    "source_path": "s3://my-raw-bucket/events/",      # where files land
    "source_format": "json",                            # parquet/csv/json
    "schema_location": "s3://my-bucket/_schemas/events",  # schema evolution tracking
    "target_table": "main.bronze.events",
    "checkpoint_path": "s3://my-bucket/_checkpoints/events",  # REQUIRED: exactly-once
}
```

### Step 3: Run
```python
StreamIngestDatabricks(cfg).run()
```

### Key: checkpoint_path is REQUIRED
Without it, data can be duplicated or lost on restart. Always use a durable path (S3/DBFS).

## AWS (Kinesis / Kafka + Glue Streaming)

### Step 1: Create a Glue Streaming job (not regular ETL)

### Step 2: Configure
```python
# In stream_ingest.py — set:
SOURCE_TYPE = "kinesis"  # or "kafka"
STREAM_NAME = "my-data-stream"  # Kinesis stream name
# OR
KAFKA_BROKERS = "broker1:9092,broker2:9092"
KAFKA_TOPIC = "my-topic"
```

### Step 3: Run
```bash
aws glue start-job-run --job-name my-streaming-job \
  --arguments '{"--SOURCE_TYPE":"kinesis","--STREAM_NAME":"my-stream","--CHECKPOINT_PATH":"s3://..."}'
```

## Best practices
- ✅ Always set `checkpointLocation` (exactly-once guarantee)
- ✅ Use `trigger(availableNow=True)` for batch-drain (process all available, then stop)
- ✅ Use `processingTime="60 seconds"` for continuous streaming
- ✅ Autoloader > plain readStream for files (handles schema evolution + scales better)
