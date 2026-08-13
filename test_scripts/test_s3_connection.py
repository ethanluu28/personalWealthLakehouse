"""
S3 connectivity test — confirms AWS credentials + IAM permissions
(ListBucket, PutObject, GetObject, DeleteObject) work on THIS machine
before running any real ingestion scripts against your bucket.

Usage:
    python test_s3_connection.py
"""

import os
import sys
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.environ["WEALTH_LAKEHOUSE_BUCKET"]
AWS_REGION = os.environ["AWS_REGION"]

TEST_KEY = f"_connection_test/{datetime.now(timezone.utc).isoformat()}.txt"
TEST_BODY = b"wealth-lakehouse connectivity check"


def run():
    print(f"Bucket:  {S3_BUCKET}")
    print(f"Region:  {AWS_REGION}\n")

    s3 = boto3.client("s3", region_name=AWS_REGION)

    # 1. Credentials resolved at all?
    try:
        identity = boto3.client("sts", region_name=AWS_REGION).get_caller_identity()
        print(f"Credentials resolved OK — IAM identity: {identity['Arn']}")
    except NoCredentialsError:
        sys.exit(
            "No AWS credentials found. boto3 looks for them in this order:\n"
            "  1. AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars\n"
            "  2. ~/.aws/credentials (created by `aws configure`)\n"
            "  3. IAM role (not applicable on a personal machine)\n"
        )

    # 2. ListBucket
    try:
        s3.list_objects_v2(Bucket=S3_BUCKET, MaxKeys=1)
        print("ListBucket OK")
    except ClientError as e:
        sys.exit(f"ListBucket FAILED: {e.response['Error']['Code']} — {e.response['Error']['Message']}")

    # 3. PutObject
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=TEST_KEY, Body=TEST_BODY)
        print(f"PutObject OK -> s3://{S3_BUCKET}/{TEST_KEY}")
    except ClientError as e:
        sys.exit(f"PutObject FAILED: {e.response['Error']['Code']} — {e.response['Error']['Message']}")

    # 4. GetObject
    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=TEST_KEY)
        assert resp["Body"].read() == TEST_BODY
        print("GetObject OK — content matches")
    except ClientError as e:
        sys.exit(f"GetObject FAILED: {e.response['Error']['Code']} — {e.response['Error']['Message']}")

    # 5. DeleteObject (cleanup)
    try:
        s3.delete_object(Bucket=S3_BUCKET, Key=TEST_KEY)
        print("DeleteObject OK — test file cleaned up")
    except ClientError as e:
        print(f"WARNING: cleanup DeleteObject failed, you may want to remove "
              f"s3://{S3_BUCKET}/{TEST_KEY} manually: {e}")

    print("\nAll checks passed — this machine can read/write your bucket.")


if __name__ == "__main__":
    run()
