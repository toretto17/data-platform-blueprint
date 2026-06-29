# 📋 common/logging — AWS — ☁️ AWS


`logger.py` provides `get_logger(name)` and `log_metric(logger, name, value, **dims)`.

- Output goes to stdout → CloudWatch (Glue ships stdout automatically).
- Structured `key=value` tail lets you query in CloudWatch Logs Insights.

```python
from aws.src.common.logging.logger import get_logger, log_metric
logger = get_logger("silver_sales")
logger.info("read complete", extra={"rows": 1234, "stage": "read"})
log_metric(logger, "rows_written", 1234, table="silver_sales")
```

Same API as `databricks/src/common/logging/logger.py` — keep in sync.


---

> 🔄 **Platform twin:** `./databricks/src/common/logging/`
