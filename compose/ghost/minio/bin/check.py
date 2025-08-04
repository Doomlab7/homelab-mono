#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.13"
# dependencies = [
#   "boto3",
#   "python-dotenv"
# ]
# ///

import boto3
import os
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("AWS_S3_ENDPOINT_URL"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)


def main():
    print("Buckets available:")
    try:
        resp = s3.list_buckets()
        for bucket in resp.get("Buckets", []):
            print(f"- {bucket['Name']}")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
