# 🔄 How to: Initial load & backfill

## Gold layer
- Set `MODE=overwrite` + `LOOKBACK_DAYS=0` in DDB/DefaultArguments
- Trigger the job → processes ALL months from Silver
- After: revert to `MODE=append` + `LOOKBACK_DAYS=60` for daily incremental

## Feature Store
- Set `BACKFILL=true` → ingests ALL gold months into the FS
- After: revert to `BACKFILL=false` (3-month rolling window daily)

## Consumption
- Set `INITIAL_LOAD=true` → loads all Gold history capped at < min(DS scored day)
- Trigger → writes the baseline
- Flip `INITIAL_LOAD=false`
- Trigger again → daily mode processes DS scored data on top

## Key rules
- Always set params BEFORE triggering, and REVERT AFTER
- Gold must be backfilled FIRST (consumption reads from gold)
- Feature Store must be backfilled BEFORE training (training reads from FS)
