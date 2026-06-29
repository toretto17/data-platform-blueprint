# ingestion

Scheduled job that computes features from upstream tables (e.g. Gold)  and writes them to a UC feature table. Supports:    • Batch mode (compute all / lookback window, then fe.write_table)    • Stream

## Files

- `feature_store_job.py`

## Platform twin

`./aws/src/feature_store/ingestion/`
