import json
import logging
import os
import boto3
from typing import Dict, Any, List
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize boto3 clients
s3_client = boto3.client("s3")
dynamodb_client = boto3.client("dynamodb")

# Constants
BATCH_SIZE = 1  # DynamoDB batch_write_item limit


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda function to read DynamoDB-formatted JSON files from S3 prefix and batch load into DynamoDB.
    
    Can process either specific files (if provided in event) or all files in S3 prefix.

    Configuration is read from environment variables:
    - S3_BUCKET: S3 bucket name
    - S3_PREFIX: S3 prefix path (optional, defaults to empty string)
    - DYNAMODB_TABLE: DynamoDB table name
    
    Event payload (optional):
    - files: List of specific filenames to process (e.g., ["test.json", "test2.json"])
    """
    try:
        # Extract parameters from environment variables
        s3_bucket = os.environ.get("S3_BUCKET")
        s3_prefix = os.environ.get("S3_PREFIX", "")
        dynamodb_table = os.environ.get("DYNAMODB_TABLE")

        if not all([s3_bucket, dynamodb_table]):
            raise ValueError(
                "Missing required environment variables: S3_BUCKET or DYNAMODB_TABLE"
            )

        logger.info(
            f"Processing files from s3://{s3_bucket}/{s3_prefix} to table {dynamodb_table}"
        )

        # Check if specific files are requested
        specific_files = event.get("files", [])
        
        if specific_files:
            logger.info(f"Processing specific files: {specific_files}")
            # Build full S3 keys for specific files
            json_files = []
            for filename in specific_files:
                if s3_prefix:
                    full_key = f"{s3_prefix}/{filename}"
                else:
                    full_key = filename
                json_files.append(full_key)
        else:
            logger.info("No specific files provided, processing all JSON files in S3 prefix")
            # List all JSON files in the S3 prefix
            json_files = list_json_files(s3_bucket, s3_prefix)

        if not json_files:
            logger.warning(f"No JSON files found in s3://{s3_bucket}/{s3_prefix}")
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {
                        "message": "No JSON files found",
                        "processed_files": 0,
                        "processed_items": 0,
                    }
                ),
            }

        logger.info(f"Found {len(json_files)} JSON files to process")

        # Read all JSON files and collect items
        all_items = []
        processed_files = 0
        failed_files = []

        for file_key in json_files:
            try:
                json_data = read_json_from_s3(s3_bucket, file_key)
                all_items.append(json_data)
                processed_files += 1
            except Exception as e:
                logger.error(f"Failed to process file {file_key}: {str(e)}")
                failed_files.append({"file": file_key, "error": str(e)})

        # Batch load items into DynamoDB
        result = batch_load_to_dynamodb(all_items, dynamodb_table)

        logger.info(
            f"Successfully processed {processed_files} files and loaded {result['processed_items']} items"
        )

        response_body = {
            "message": "Data loaded successfully",
            "processed_files": processed_files,
            "processed_items": result["processed_items"],
            "failed_items": result["failed_items"],
            "table_name": dynamodb_table,
        }

        if failed_files:
            response_body["failed_files"] = failed_files

        return {"statusCode": 200, "body": json.dumps(response_body)}

    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e), "message": "Failed to process data"}),
        }


def read_json_from_s3(bucket: str, key: str) -> Dict[str, Any]:
    """
    Read and parse JSON file from S3.

    Args:
        bucket: S3 bucket name
        key: S3 object key

    Returns:
        Parsed JSON data as dictionary

    Raises:
        ClientError: If S3 operation fails
        json.JSONDecodeError: If JSON is malformed
    """
    try:
        logger.info(f"Reading file from S3: s3://{bucket}/{key}")

        response = s3_client.get_object(Bucket=bucket, Key=key)
        file_content = response["Body"].read().decode("utf-8")

        # Parse JSON content
        json_data = json.loads(file_content)
        logger.info("Successfully parsed JSON from S3")

        return json_data

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "NoSuchKey":
            raise FileNotFoundError(f"File not found: s3://{bucket}/{key}")
        elif error_code == "NoSuchBucket":
            raise FileNotFoundError(f"Bucket not found: {bucket}")
        else:
            raise ClientError(
                f"S3 error: {e.response['Error']['Message']}", e.operation_name
            )

    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(
            f"Invalid JSON format in file s3://{bucket}/{key}: {str(e)}", e.doc, e.pos
        )


def list_json_files(bucket: str, prefix: str) -> List[str]:
    """
    List all JSON files in S3 bucket with given prefix.

    Args:
        bucket: S3 bucket name
        prefix: S3 prefix (folder path)

    Returns:
        List of S3 object keys for JSON files

    Raises:
        ClientError: If S3 operation fails
    """
    try:
        logger.info(f"Listing JSON files in s3://{bucket}/{prefix}")

        json_files = []
        paginator = s3_client.get_paginator("list_objects_v2")

        # Paginate through all objects in the prefix
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                key = obj["Key"]
                # Filter only .json files and exclude directories
                if key.lower().endswith(".json") and not key.endswith("/"):
                    json_files.append(key)

        logger.info(f"Found {len(json_files)} JSON files")
        return json_files

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "NoSuchBucket":
            raise FileNotFoundError(f"Bucket not found: {bucket}")
        else:
            raise ClientError(
                f"S3 error: {e.response['Error']['Message']}", e.operation_name
            )


def batch_load_to_dynamodb(
    items: List[Dict[str, Any]], table_name: str
) -> Dict[str, int]:
    """
    Batch load DynamoDB-formatted items into DynamoDB table.

    Args:
        items: List of DynamoDB-formatted item data
        table_name: Target DynamoDB table name

    Returns:
        Dictionary with processing results including processed and failed counts

    Raises:
        ClientError: If DynamoDB operation fails
    """
    try:
        logger.info(f"Batch loading {len(items)} items to DynamoDB table: {table_name}")

        processed_items = 0
        failed_items = 0

        # Process items in batches of 25 (DynamoDB limit)
        for i in range(0, len(items), BATCH_SIZE):
            batch = items[i : i + BATCH_SIZE]

            # Prepare batch write request with validation
            valid_items = []
            for item in batch:
                if _is_valid_dynamodb_format(item):
                    valid_items.append({"PutRequest": {"Item": item}})
                else:
                    logger.error("Invalid DynamoDB format for item: %s", item)
                    failed_items += 1

            request_items = {table_name: valid_items}

            if not valid_items:
                continue

            # Execute batch write
            response = dynamodb_client.batch_write_item(RequestItems=request_items)

            # Track processed items
            batch_processed = len(valid_items)
            unprocessed_items = response.get("UnprocessedItems", {})

            if unprocessed_items:
                unprocessed_count = len(unprocessed_items.get(table_name, []))
                batch_processed -= unprocessed_count
                failed_items += unprocessed_count
                logger.warning("Batch has %d unprocessed items", unprocessed_count)

            processed_items += batch_processed

            logger.info(
                f"Processed batch {i // BATCH_SIZE + 1}: {processed_items} items loaded"
            )

        logger.info(
            f"Batch load complete: {processed_items} succeeded, {failed_items} failed"
        )

        return {"processed_items": processed_items, "failed_items": failed_items}

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "ResourceNotFoundException":
            raise ClientError(
                f"DynamoDB table not found: {table_name}", e.operation_name
            )
        elif error_code == "ValidationException":
            raise ClientError(
                f"Invalid item format: {e.response['Error']['Message']}",
                e.operation_name,
            )
        else:
            raise ClientError(
                f"DynamoDB error: {e.response['Error']['Message']}", e.operation_name
            )


def _is_valid_dynamodb_format(data: Dict[str, Any]) -> bool:
    """
    Validate that data is in DynamoDB attribute format.

    Args:
        data: Data to validate

    Returns:
        True if valid DynamoDB format, False otherwise
    """
    if not isinstance(data, dict):
        return False

    # Check if all values are DynamoDB attribute value format
    valid_types = {"S", "N", "B", "SS", "NS", "BS", "M", "L", "NULL", "BOOL"}

    for key, value in data.items():
        if not isinstance(value, dict):
            return False

        # Each attribute should have exactly one type key
        if len(value) != 1:
            return False

        type_key = next(iter(value.keys()))
        if type_key not in valid_types:
            return False

        # For Map (M) type, recursively validate nested structure
        if type_key == "M" and isinstance(value["M"], dict):
            if not _is_valid_dynamodb_format(value["M"]):
                return False

    return True
