# <Job Name>

## Purpose
<One-line description>

## Source
- Table/path: `CHANGE_ME`

## Target
- Table/path: `CHANGE_ME`
- Partition: `CHANGE_ME`

## Run
```bash
# AWS
aws glue start-job-run --job-name CHANGE_ME --arguments '{"--MODE":"append"}'
# Databricks
databricks jobs run-now --job-id CHANGE_ME
```

## DQ checks
- [ ] Row count > X
- [ ] PK not null
- [ ] Freshness < 48h
