import os

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()


def check_s3_bucket():
    print("Checking S3 bucket...")
    # Initialize S3 client
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name="ap-southeast-1",
    )

    bucket_name = os.environ.get("S3_BUCKET_NAME")

    try:
        # Check if bucket exists
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' exists and is accessible!")

        # List objects in bucket
        print("\nListing bucket contents:")
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if "Contents" in response:
            for obj in response["Contents"]:
                print(f"- {obj['Key']} ({obj['Size']} bytes)")
        else:
            print("Bucket is empty")

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            print(f"❌ Bucket '{bucket_name}' does not exist")
        elif error_code == "403":
            print(
                f"❌ Access denied to bucket '{bucket_name}'. Check your AWS credentials and permissions."
            )
        else:
            print(f"❌ Error accessing bucket: {e}")


if __name__ == "__main__":
    # Make sure you have boto3 installed: pip install boto3
    check_s3_bucket()
