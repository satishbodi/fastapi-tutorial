import boto3
import csv
import io
import os
import json
import threading
import time
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
REQUIRED_COLUMNS = ["id", "name", "email"]

session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )

# Initialize AWS clients
s3_client = session.client("s3", region_name=AWS_REGION)
sqs_client = session.client("sqs", region_name=AWS_REGION)

class ValidationResult(BaseModel):
    s3_key: str
    valid: bool
    errors: Optional[List[str]] = []
    row_count: Optional[int] = None

def validate_csv_from_s3(s3_key: str) -> ValidationResult:
    try:
        s3_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        csv_content = s3_obj['Body'].read().decode('utf-8')
        reader = csv.DictReader(io.StringIO(csv_content))
        missing_columns = [col for col in REQUIRED_COLUMNS if col not in reader.fieldnames]
        if missing_columns:
            return ValidationResult(s3_key=s3_key, valid=False, errors=[f"Missing columns: {', '.join(missing_columns)}"])
        row_errors = []
        row_count = 0
        for idx, row in enumerate(reader):
            print(f"Validating row - {row['id']}, {row['name']}, {row['email']}")
            row_count += 1
            try:
                int(row["id"])
            except (ValueError, KeyError):
                row_errors.append(f"Row {idx+2}: 'id' must be an integer.")
            if "email" in row and "@" not in row["email"]:
                row_errors.append(f"Row {idx+2}: 'email' appears invalid.")
        if row_errors:
            return ValidationResult(s3_key=s3_key, valid=False, errors=row_errors, row_count=row_count)
        return ValidationResult(s3_key=s3_key, valid=True, row_count=row_count)

    except s3_client.exceptions.NoSuchKey:
        return ValidationResult(s3_key=s3_key, valid=False, errors=["CSV not found in S3"])
    except Exception as e:
        return ValidationResult(s3_key=s3_key, valid=False, errors=[str(e)])

@app.post("/process-sqs-batch/", response_model=List[ValidationResult])
def process_sqs_batch(max_messages: int = 5):
    """
    Polls SQS for messages and processes S3 CSV validation.
    Each SQS message should contain JSON {"s3_key": "<key>"}
    """
    results = []
    messages_to_delete = []

    try:
        response = sqs_client.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=5
        )
        messages = response.get("Messages", [])
        for msg in messages:
            try:
                body = json.loads(msg["Body"])
                # For S3 notification events:
                s3_key = body["Records"][0]["s3"]["object"]["key"]
                print(f"Processing CSV File: {s3_key}")
                result = validate_csv_from_s3(s3_key)
                print(f"Validation result for {s3_key}: {result.valid}, Errors: {result.errors}")
                results.append(result)
                # Add to delete batch if processed
                messages_to_delete.append({"Id": msg["MessageId"], "ReceiptHandle": msg["ReceiptHandle"]})
            except Exception as e:
                results.append(ValidationResult(s3_key="unknown", valid=False, errors=[f"Failed to process message: {str(e)}"]))
        if messages_to_delete:
            sqs_client.delete_message_batch(
                QueueUrl=SQS_QUEUE_URL,
                Entries=messages_to_delete
            )
        return results

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Optional: Background worker to poll SQS continuously
def sqs_background_poll(interval: int = 10):
    while True:
        print("Polling for new messages in SQS...")
        process_sqs_batch()
        time.sleep(interval)

def start_sqs_poll_worker():
    print("Starting SQS poll worker...")
    worker = threading.Thread(target=sqs_background_poll, args=(10,), daemon=True)
    worker.start()

# Uncomment if you want FastAPI to start background poller on app startup
# @app.on_event("startup")
# def startup_event():
#     start_sqs_poll_worker()


@app.get("/")
async def root():
    start_sqs_poll_worker()
    return {"message": "Hello from FastAPI on EKS!!"}
