import asyncio
import os

from app.s3 import list_files, delete_file, download_file, move_file
from app.database import file_has_a_job_in_db, get_job_status, set_job_status
from app.file_enqueuer import FileEnqueuer
from app.config import PAUSE, POLLING_INTERVAL

files_queued = []


async def enqueue_new_files():
    while True:
        # Pause if env variable is set to pause
        if PAUSE:
            print("File enqueuer is paused.")
            await asyncio.sleep(POLLING_INTERVAL)
            continue

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
            print("File enqueuer is paused. Skipping this cycle.")
            await asyncio.sleep(POLLING_INTERVAL)
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

            # Otherwise, enqueue the file rows
            # Create file processor
            processor = FileEnqueuer(
                rabbitmq_host="rabbitmq.apps.cansin.net",
                username="cca",
                password="2q_0o9nc$XCP_zM",
            )
            # Connect to RabbitMQ
            if not processor.connect():
                print("ERROR: Could not connect to RabbitMQ")

            # Download the file locally
            local_file_name = os.path.basename(item["Key"])
            local_file_path_relative = os.path.join("tmp/", local_file_name)
            local_file_path = os.path.abspath(local_file_path_relative)
            download_file(item["Key"], local_file_path)
            print(f"Downloaded {item['Key']} to {local_file_path}")

            # Process the file
            result = processor.process_csv_file(local_file_path)

            if result["status"] == "success":
                print(f"✓ Successfully processed {result['filename']}")
                print(f"  Rows published: {result['rows_published']}")
                print(f"  Queue created: {result['queue_name']}")
                print(f"  Processing time: {result['processing_time_seconds']}s")
            else:
                print(
                    f"✗ Failed to process {result['filename']}: {result.get('error', 'Unknown error')}"
                )

            # Add file to the enqueued list to avoid re-enqueuing
            files_queued.append(item)

            # Update its status in db
            set_job_status(item["Key"], "file_queued")

            # Delete file from local
            try:
                os.remove(local_file_path)
            except Exception as e:
                print(f"Error deleting local file {local_file_path}: {e}")

            # Delete remote file from S3
            # delete_file(item["Key"])
            move_file(
                item["Key"],
                item["Key"].replace("validation/in-progress/", "validation/queued/"),
            )

            # Log
            print(f'Enqueued file: {item["Key"]}')

        await asyncio.sleep(POLLING_INTERVAL)
