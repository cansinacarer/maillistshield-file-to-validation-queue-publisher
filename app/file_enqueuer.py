# CSV File Processor Microservice

import pika
import json
import time
import csv
import io
import os
from datetime import datetime

from app.config import (
    RABBITMQ_HOST,
    RABBITMQ_VHOST,
    RABBITMQ_USERNAME,
    RABBITMQ_PASSWORD,
)
from app.utilities.logging import logger


class FileEnqueuer:
    """
    Processor that reads CSV files and publishes rows to RabbitMQ
    """

    def __init__(
        self,
        rabbitmq_host=RABBITMQ_HOST,
        username="",
        password="",
        rabbitmq_port=5672,
        queue_prefix="batch_validation",
    ):
        self.rabbitmq_host = rabbitmq_host
        self.rabbitmq_port = rabbitmq_port
        self.username = username
        self.password = password
        self.queue_prefix = queue_prefix
        self.connection = None
        self.channel = None

    def connect(self):
        """Connect to RabbitMQ with retry logic"""
        max_retries = 5
        retry_delay = 5  # seconds

        for attempt in range(max_retries):
            try:
                credentials = pika.PlainCredentials(self.username, self.password)
                parameters = pika.ConnectionParameters(
                    host=self.rabbitmq_host,
                    port=self.rabbitmq_port,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300,
                )
                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()
                logger.debug(
                    f"Connected to RabbitMQ at {self.rabbitmq_host}:{self.rabbitmq_port}"
                )
                return True
            except Exception as e:
                logger.warning(
                    f"Connection attempt {attempt + 1}/{max_retries} failed: {e}"
                )
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    logger.error("Failed to connect to RabbitMQ after all retries.")
                    return False

    def disconnect(self):
        """Safely disconnect from RabbitMQ"""
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                logger.debug("Disconnected from RabbitMQ")
        except Exception as e:
            logger.warning(f"Error during disconnect: {e}")

    def process_csv_file(self, filepath):
        """
        Process CSV file and publish rows to dedicated queue

        Args:
            filepath: Path to CSV file

        Returns:
            Dict with processing results and statistics
        """
        filename = os.path.basename(filepath)

        if not self.connection:
            raise Exception("Not connected to RabbitMQ.")

        # Create safe queue name
        safe_filename = (
            filename.replace(".", "_")
            .replace("/", "_")
            .replace(" ", "_")
            .replace("-", "_")
            .lower()
        )
        queue_name = f"{self.queue_prefix}_{safe_filename}"

        try:
            # Read and validate CSV file
            logger.debug(f"Reading CSV file: {filepath}")
            with open(filepath, "r", encoding="utf-8") as file:
                csv_content = file.read()

            # Parse CSV to validate and count rows
            csv_reader = csv.DictReader(io.StringIO(csv_content))
            rows = list(csv_reader)

            if not rows:
                logger.warning(f"No data rows found in {filename}")
                return {
                    "filename": filename,
                    "filepath": filepath,
                    "rows_processed": 0,
                    "status": "warning",
                    "message": "No data rows found",
                }

            # Declare durable queue for this file
            self.channel.queue_declare(queue=queue_name, durable=True)
            logger.debug(f"Created queue: {queue_name}")

            # Publish each row as individual message
            published_count = 0
            start_time = time.time()

            for row_num, row in enumerate(rows, 1):
                message = {
                    "messageId": f"{filename}_row_{row_num}_{int(time.time() * 1000)}",
                    "filename": filename,
                    "filepath": filepath,
                    "rowNumber": row_num,
                    "totalRows": len(rows),
                    "queueName": queue_name,
                    "processedAt": datetime.utcnow().isoformat(),
                    "email": row["Email"],
                }

                # Publish with persistence
                self.channel.basic_publish(
                    exchange="",  # Use default exchange (direct to queue)
                    routing_key=queue_name,
                    body=json.dumps(message),
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # Persistent message
                        message_id=message["messageId"],
                        headers={
                            "filename": filename,
                            "rowNumber": row_num,
                            "totalRows": len(rows),
                        },
                    ),
                )
                published_count += 1

                # Log progress periodically for large files
                if published_count % 1000 == 0:
                    logger.debug(
                        f"Published {published_count}/{len(rows)} rows from {filename}"
                    )

            processing_time = time.time() - start_time

            result = {
                "filename": filename,
                "filepath": filepath,
                "queue_name": queue_name,
                "total_rows": len(rows),
                "rows_published": published_count,
                "columns": list(rows[0].keys()) if rows else [],
                "processing_time_seconds": round(processing_time, 2),
                "processed_at": datetime.utcnow().isoformat(),
                "status": "success",
            }

            logger.debug(
                f"Successfully processed {filename}: {published_count} rows -> {queue_name}"
            )
            return result

        except FileNotFoundError:
            error_msg = f"File not found: {filepath}"
            logger.error(error_msg)
            return {
                "filename": filename,
                "filepath": filepath,
                "error": error_msg,
                "status": "error",
            }
        except csv.Error as e:
            error_msg = f"CSV parsing error in {filename}: {e}"
            logger.error(error_msg)
            return {
                "filename": filename,
                "filepath": filepath,
                "error": error_msg,
                "status": "error",
            }
        except Exception as e:
            error_msg = f"Error processing {filename}: {e}"
            logger.error(error_msg)
            return {
                "filename": filename,
                "filepath": filepath,
                "error": error_msg,
                "status": "error",
            }
