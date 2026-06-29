# 🔧 common/utils — Databricks — 🧱 Databricks


`etl_utils.py` — Delta-flavored twin of the AWS module. Same public API:
- `EarlyExitCheck.is_empty(df)`
- `MetadataFreshnessManager(marker_table)` — watermark in a Delta table
- `get_writer("delta")` → `DeltaWriter` with append / dynamic-overwrite / merge(upsert)

```python
from databricks.src.common.utils.etl_utils import EarlyExitCheck, get_writer
get_writer().write(df, "main.silver.sales", merge_keys=["id"], mode="merge")
```


---

> 🔄 **Platform twin:** `./aws/src/common/utils/`
