import json
import base64
from pymongo import MongoClient, UpdateOne
from datetime import datetime, timedelta

# MongoDB Configuration
MONGO_URI = "mongodb+srv://s3978598:minhphan123@eeet2574.n8www.mongodb.net/?retryWrites=true&w=majority&appName=EEET2574"
DATABASE_NAME = "streaming_data"
COLLECTION_NAME = "weather"

# Create MongoDB client
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

def log_message(message):
    """Log messages with a timestamp."""
    current_time_utc = datetime.utcnow()
    current_time_local = current_time_utc + timedelta(hours=7)
    formatted_time = current_time_local.strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{formatted_time}] {message}")

def lambda_handler(event, context):
    """Lambda function handler for Kinesis stream events."""
    operations = []  # List to store MongoDB operations

    for record in event["Records"]:
        try:
            # Decode Base64-encoded Kinesis data
            raw_data = record["kinesis"]["data"]
            payload = base64.b64decode(raw_data).decode("utf-8")
            log_message(f"Raw Payload: {payload}")

            # Parse JSON data
            json_data = json.loads(payload)
            time = json_data.get("time")
            airport_id = json_data.get("airport_id")

            # Ensure required fields are present
            if not time or not airport_id:
                log_message(f"Missing required fields 'airport_id' or 'time'. Skipping record: {json_data}")
                continue

            # Prepare the update operation
            operations.append(
                UpdateOne(
                    {"time": time, "airport_id": airport_id},  # Filter by airport_id and time
                    {"$set": json_data},                       # Update with new data
                    upsert=True                                # Insert if not exists
                )
            )

        except json.JSONDecodeError as e:
            log_message(f"JSON decoding error: {e}. Skipping record.")
        except Exception as e:
            log_message(f"Error processing record: {e}")

    # Perform bulk write (insert or update operations)
    if operations:
        try:
            result = collection.bulk_write(operations)
            log_message(
                f"{result.modified_count} records updated, {result.upserted_count} records inserted successfully."
            )
        except Exception as e:
            log_message(f"Error performing bulk write to MongoDB: {e}")

    return {"statusCode": 200, "body": "Records processed successfully"}
