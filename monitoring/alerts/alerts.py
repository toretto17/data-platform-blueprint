"""
================================================================================
ALERTING — SNS + Slack webhook  [AWS]
================================================================================
Send alerts on pipeline failures / DQ issues / drift detection.
Customize: SNS_TOPIC_ARN, SLACK_WEBHOOK_URL.
================================================================================
"""
import json, logging, os
import boto3
from urllib.request import Request, urlopen

logger = logging.getLogger("alerts")

SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "arn:aws:sns:CHANGE_ME:CHANGE_ME:CHANGE_ME")
SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK_URL", "")
REGION = os.environ.get("REGION", "ap-southeast-1")


def send_sns(subject: str, message: str):
    boto3.client("sns", region_name=REGION).publish(
        TopicArn=SNS_TOPIC_ARN, Subject=subject[:100], Message=message)
    logger.info(f"SNS alert: {subject}")


def send_slack(text: str, channel: str = "#alerts"):
    if not SLACK_WEBHOOK:
        logger.warning("SLACK_WEBHOOK_URL not set — skipping")
        return
    payload = json.dumps({"channel": channel, "text": text}).encode()
    urlopen(Request(SLACK_WEBHOOK, data=payload, headers={"Content-Type": "application/json"}))
    logger.info(f"Slack alert sent to {channel}")


def alert(title: str, body: str, severity: str = "HIGH"):
    """Send to both SNS + Slack (if configured)."""
    msg = f"[{severity}] {title}\n{body}"
    send_sns(title, msg)
    send_slack(msg)
