import asyncio
import os

from app.utilities.s3 import list_files, download_file, move_file
from app.utilities.database import file_has_a_job_in_db, get_job_status, set_job_status
from app.utilities.logging import logger
from app.file_enqueuer import FileEnqueuer
from app.config import (
    PAUSE,
    POLLING_INTERVAL,
)


async def enqueue_new_files():
    while True:
        # Pause if env variable is set to pause
        if PAUSE:
            logger.info("File enqueuer is paused.")
            await asyncio.sleep(POLLING_INTERVAL)
            continue

        all_files = list_files(prefix="validation/in-progress/")
        new_files = []

        # Pick the new files from
        for item in all_files:
            # Do not include the folder itself
            if item["Key"] == "validation/in-progress/":
                continue
            new_files.append(item)

        if len(new_files) == 0:
            logger.debug("File enqueuer did not find any files.")
            await asyncio.sleep(POLLING_INTERVAL)
            continue

        logger.debug(
            f"{len(new_files)} new files are found: {', '.join([item['Key'] for item in new_files])}"
        )

        # Looping separately from the above to save db queries
        # by only checking the db status of files that are not
        # in the queue of this worker
        for item in new_files:
            # Skip file if we don't find a matching db record
            if not file_has_a_job_in_db(item["Key"]):
                logger.debug(f'{item["Key"]} does not have a db record, skipping it.')
                continue

            # Skip file if db says the file is not file_accepted
            if get_job_status(item["Key"]) != "file_accepted":
                logger.debug(
                    f'{item["Key"]} has a db record but it is not file_accepted, skipping it.'
                )
                continue

            # Otherwise, enqueue the file rows
            # Create file processor
            processor = FileEnqueuer()

            # Download the file locally
            local_file_name = os.path.basename(item["Key"])
            local_file_path_relative = os.path.join("tmp/", local_file_name)
            local_file_path = os.path.abspath(local_file_path_relative)
            download_file(item["Key"], local_file_path)
            logger.debug(f"Downloaded {item['Key']} to {local_file_path}")

            # Process the file
            result = processor.process_csv_file(local_file_path)

            if result["status"] != "success":
                logger.error(
                    f"Failed to process {result['filename']}: {result.get('error', 'Unknown error')}"
                )

            # Update its status in db
            set_job_status(item["Key"], "file_queued")

            # Delete file from local
            try:
                os.remove(local_file_path)
            except Exception as e:
                logger.error(f"Error deleting local file {local_file_path}: {e}")

            # Delete remote file from S3
            move_file(
                item["Key"],
                item["Key"].replace("validation/in-progress/", "validation/queued/"),
            )

            # Log
            logger.debug(f'Enqueued file: {item["Key"]}')

        await asyncio.sleep(POLLING_INTERVAL)
