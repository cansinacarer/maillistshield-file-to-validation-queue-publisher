import asyncio
import datetime
import os
from app.config import (
    appTimezone,
    # RETENTION_PERIOD_FOR_ORPHAN_FILES,
    S3_BUCKET_NAME,
    s3,
    BATCH_SIZE,
)
from app.database import file_has_a_job_in_db, get_job_status, set_job_status

files_queued = []


# Returns the list of newly accepted files
def list_files():
    # Check if there are any new files in the S3 bucket
    s3_response = s3.meta.client.list_objects_v2(
        Bucket=S3_BUCKET_NAME, Prefix="validation/in-progress/"
    )

    return s3_response.get("Contents", [])


async def report_file_progress():
    while True:
        print("Reporting file progress")
        await asyncio.sleep(10)
        pass


async def enqueue_new_files():
    while True:
        all_files = list_files()
        new_files = []

        # Pick the new files from
        for item in all_files:
            # Do not include the files already queued
            if item in files_queued:
                continue
            # Do not include the folder itself
            if item["Key"] == "validation/in-progress/":
                continue
            new_files.append(item)

        print(
            f"{len(new_files)} new files are found (new to this worker's queue): {', '.join([item['Key'] for item in new_files])}"
        )

        if len(new_files) == 0:
            continue

        # Looping separately from the above to save db queries
        # by only checking the db status of files that are not
        # in the queue of this worker
        for item in new_files:
            # Skip file if we don't find a matching db record
            if not file_has_a_job_in_db(item["Key"]):
                print(f'{item["Key"]} does not have a db record, skipping it.')
                continue

            # Skip file if db says the file is not file_accepted
            if get_job_status(item["Key"]) != "file_accepted":
                print(
                    f'{item["Key"]} has a db record but it is not file_accepted, skipping it.'
                )
                continue

            # Otherwise, enqueue the file
            files_queued.append(item)

            # Update its status in db
            set_job_status(item["Key"], "queued")

            # Log
            print(f'Enqueued file: {item["Key"]}')

        await asyncio.sleep(10)


# async def validation_worker()


def delete_file(key):
    objects = [{"Key": key}]
    try:
        s3.Bucket(S3_BUCKET_NAME).delete_objects(Delete={"Objects": objects})
    except Exception as e:
        print("Error: ", e)


def download_file(key_name, local_name):
    file_path = os.path.join(os.path.dirname(__file__), local_name)

    try:
        s3.Bucket(S3_BUCKET_NAME).download_file(key_name, file_path)
    except Exception as e:
        print("Error: ", e)


def upload_csv_buffer(csv_buffer, file_name):
    try:
        s3.meta.client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key="validation/in-progress/" + file_name,
            Body=csv_buffer.getvalue(),
        )
    except Exception as e:
        print("Error: ", e)
