"""
================================================================================
ORCHESTRATION — Step Functions + EventBridge  [AWS]
================================================================================
Purpose: Programmatically create/update Step Functions (ETL pipelines) and
         EventBridge schedules. Complement to Terraform (use this for dynamic/
         runtime orchestration; Terraform for static infrastructure).

Contents:
    - StepFunctionManager: create/update/trigger/poll a state machine
    - EventBridgeScheduler: create/update cron schedules that trigger SFs
    - build_etl_sf_definition(): helper to build a linear ETL SF JSON

Best practices:
    - SF for ETL orchestration (framework SF pattern from production)
    - EventBridge for scheduling (cron or rate expressions)
    - Never hardcode ARNs — resolve from config/env variables
    - Add error handling (Catch/Retry) to every Task state
    - Use Map state for parallel fan-out (per product/partition)

Customize: SF_NAME, ROLE_ARN, schedule cron, pipeline steps.
Databricks twin: databricks/src/orchestration/orchestration.py (Databricks Workflows SDK)
Version : 2026-06-29
================================================================================
"""
import json
import logging
import time
from typing import List, Optional

import boto3

logger = logging.getLogger("orchestration_aws")


class StepFunctionManager:
    REGION: str = "ap-southeast-1"

    def __init__(self, region: Optional[str] = None):
        self.sfn = boto3.client("stepfunctions", region_name=region or self.REGION)

    def create_or_update(self, name: str, definition: dict, role_arn: str) -> str:
        """Create or update a state machine. Returns the ARN."""
        def_str = json.dumps(definition)
        try:
            resp = self.sfn.describe_state_machine(stateMachineArn=self._arn(name))
            self.sfn.update_state_machine(stateMachineArn=resp["stateMachineArn"],
                                           definition=def_str, roleArn=role_arn)
            logger.info(f"updated SF: {name}")
            return resp["stateMachineArn"]
        except self.sfn.exceptions.StateMachineDoesNotExist:
            resp = self.sfn.create_state_machine(name=name, definition=def_str,
                                                  roleArn=role_arn, type="STANDARD")
            logger.info(f"created SF: {name}")
            return resp["stateMachineArn"]

    def trigger(self, name: str, input_payload: dict) -> str:
        """Start an execution. Returns execution ARN."""
        arn = self._arn(name)
        resp = self.sfn.start_execution(stateMachineArn=arn, input=json.dumps(input_payload))
        logger.info(f"triggered {name}: {resp['executionArn']}")
        return resp["executionArn"]

    def poll_execution(self, execution_arn: str, poll_interval: int = 15) -> str:
        """Poll until terminal state. Returns SUCCEEDED/FAILED/TIMED_OUT/ABORTED."""
        while True:
            resp = self.sfn.describe_execution(executionArn=execution_arn)
            status = resp["status"]
            if status in ("SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"):
                logger.info(f"execution {status}: {execution_arn}")
                return status
            time.sleep(poll_interval)

    def _arn(self, name: str) -> str:
        account = boto3.client("sts").get_caller_identity()["Account"]
        return f"arn:aws:states:{self.REGION}:{account}:stateMachine:{name}"


class EventBridgeScheduler:
    REGION: str = "ap-southeast-1"

    def __init__(self, region: Optional[str] = None):
        self.eb = boto3.client("events", region_name=region or self.REGION)

    def create_schedule(self, rule_name: str, cron_expression: str,
                        target_sf_arn: str, sf_role_arn: str,
                        input_payload: Optional[dict] = None):
        """Create/update an EventBridge rule → Step Function target.
        cron_expression: AWS format e.g. 'cron(0 18 * * ? *)' (6PM UTC daily)."""
        self.eb.put_rule(Name=rule_name, ScheduleExpression=cron_expression,
                         State="ENABLED")
        self.eb.put_targets(Rule=rule_name, Targets=[{
            "Id": "sf-target",
            "Arn": target_sf_arn,
            "RoleArn": sf_role_arn,
            "Input": json.dumps(input_payload or {}),
        }])
        logger.info(f"schedule created: {rule_name} → {target_sf_arn} ({cron_expression})")


def build_etl_sf_definition(steps: List[dict]) -> dict:
    """Build a simple linear Step Function definition from a list of step configs.
    Each step: {"name": "...", "job_name": "..."} → invokes the framework SF.
    CHANGE_ME: adapt to your framework SF ARN."""
    states = {}
    step_names = [s["name"] for s in steps]
    for i, s in enumerate(steps):
        next_state = step_names[i + 1] if i < len(steps) - 1 else "Success"
        states[s["name"]] = {
            "Type": "Task",
            "Resource": "arn:aws:states:::states:startExecution.sync:2",
            "Parameters": {
                "StateMachineArn": "arn:aws:states:CHANGE_ME:CHANGE_ME:stateMachine:sfn-bnic-aii-fw-transformation",
                "Input": {"job_name": s["job_name"], "dl_date.$": "$.dl_date"},
            },
            "Next": next_state,
            "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Failed"}],
        }
    states["Success"] = {"Type": "Succeed"}
    states["Failed"] = {"Type": "Fail", "Cause": "A pipeline step failed."}
    return {"StartAt": step_names[0], "States": states}


if __name__ == "__main__":
    # Example: build + deploy a 3-step ETL pipeline SF
    defn = build_etl_sf_definition([
        {"name": "RunSilver", "job_name": "silver_sales_pipeline"},
        {"name": "RunGold", "job_name": "gold_sales_pipeline"},
        {"name": "RunConsumption", "job_name": "consumption_sales_pipeline"},
    ])
    # sfm = StepFunctionManager()
    # sfm.create_or_update("my-etl-pipeline", defn, "arn:aws:iam::CHANGE_ME:role/sfn-exec")
    # sfm.trigger("my-etl-pipeline", {"dl_date": "2026-06-29"})
