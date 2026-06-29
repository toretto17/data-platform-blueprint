# 👁️ monitoring/


| Subfolder | Purpose |
|---|---|
| `alerts/alerts.py` | Send alerts via SNS + Slack webhook |
| `cloudwatch/pipeline_monitor.py` | Pipeline health metrics (existed) |
| `dashboards/` | CloudWatch / Databricks SQL dashboard JSON templates |
| `metrics/` | Custom metric publishers (DQ, drift, SLA) |

## Usage
```python
from monitoring.alerts.alerts import alert
alert("Pipeline Failed", "Gold sales mart timed out after 360min", severity="HIGH")
```
