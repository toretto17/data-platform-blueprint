"""Starter DQ config. See aws/src/common/validations/dq_framework.py or databricks/src/common/validations/dq_framework.py.
from <platform>.src.common.validations.dq_framework import DQConfig, DQCheck, Severity
my_dq = DQConfig(table_name="CHANGE_ME", checks=[
    DQCheck("rows", "row_count", Severity.CRITICAL, {"min_count": 100}),
    DQCheck("pk_null", "completeness", Severity.HIGH, {"column": "id", "max_null_pct": 0.0}),
])
"""
