import json
import boto3
import requests
from datetime import datetime
import os

# Initialize S3 client
s3 = boto3.client("s3")


def lambda_handler(event, context):
    try:
        # Fetch bucket name from environment variables
        bucket_name = os.environ["BUCKET_NAME"]

        # Fetch data from CoinCap API
        response = requests.get("https://api.coincap.io/v2/assets")
        data = response.json()

        # Generate a unique file name with timestamp
        file_name = f"coincap-data-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.json"

        # Upload the data to the specified S3 bucket
        s3.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json.dumps(data),
            ContentType="application/json",
        )

        return {"statusCode": 200, "body": json.dumps("Data stored successfully")}
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps(f"Error fetching data: {str(e)}")}
