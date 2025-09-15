import os

from app.config import (
    S3_BUCKET_NAME,
    s3,
)
from app.utilities.logging import logger


# Returns the list of newly accepted files
def list_files(prefix=""):
    # Check if there are any new files in the S3 bucket
    s3_response = s3.meta.client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)

    return s3_response.get("Contents", [])


def delete_file(key):
    objects = [{"Key": key}]
    try:
        s3.Bucket(S3_BUCKET_NAME).delete_objects(Delete={"Objects": objects})
    except Exception as e:
        logger.error(f"Error deleting file: {e}", extra={"file_key": key})


def download_file(key, local_name):
    file_path = os.path.join(os.path.dirname(__file__), local_name)

    try:
        s3.Bucket(S3_BUCKET_NAME).download_file(key, file_path)
    except Exception as e:
        logger.error(f"Error downloading file: {e}", extra={"file_key": key})


def move_file(source_key, destination_key):
    copy_source = {"Bucket": S3_BUCKET_NAME, "Key": source_key}
    try:
        s3.meta.client.copy(copy_source, S3_BUCKET_NAME, destination_key)
        delete_file(source_key)
    except Exception as e:
        logger.error(f"Error moving file: {e}", extra={"file_key": key})
