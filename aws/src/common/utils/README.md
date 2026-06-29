# common/utils — AWS

- `etl_utils.py` — EarlyExitCheck, MetadataFreshnessManager, write strategies
  (spark_native, glue_catalog, delta, iceberg), partition mgmt. `get_writer(strategy)`.
- `production_patterns.py` — merge, cache, dedup, retry, optional-arg parsing, backfill.

Job code imports e.g.:
```python
from aws.src.common.utils.etl_utils import EarlyExitCheck, get_writer
```
