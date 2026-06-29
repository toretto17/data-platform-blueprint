# common/logging — Databricks

Same API as the AWS logger (`get_logger`, `log_metric`). Output goes to the
Databricks driver log. `log_metric` also mirrors numeric values to MLflow when
a run is active (useful in ML jobs).

```python
from databricks.src.common.logging.logger import get_logger, log_metric
logger = get_logger("silver_sales")
logger.info("read complete", extra={"rows": 1234, "stage": "read"})
```
