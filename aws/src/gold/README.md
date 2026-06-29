# 🥇 Gold — aws — ☁️ AWS


Business-ready aggregates (Silver → Gold).

| File | Purpose |
|---|---|
| `marts/gold_job.py` | Base Gold job — daily aggregates, period (MTD/YTD/WTD) windows, comparison periods, zero-fill, round, write |
| `dq/gold_dq.py` | Gold DQ config (business rule, reconciliation) |

Production bug-patterns enforced: window PARTITION BY must include ALL dims;
dedup group-level cols before SUM; rates = ratio-of-sums (not sum-of-ratios);
round floats before write.


---

> 🔄 **Platform twin:** `./databricks/src/gold/`
