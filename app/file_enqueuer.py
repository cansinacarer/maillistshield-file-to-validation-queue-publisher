# CSV File Processor Microservice

import pika
import json
import time
import csv
import io
import os
from datetime import datetime

from app.utilities.rabbitmq import QueueAgent
from app.utilities.logging import logger


class FileEnqueuer:
    """
    Processor that reads CSV files and publishes rows to RabbitMQ
    """

    def __init__(self):
        self.queue_prefix = "batch_validation"
        self.queue_agent = QueueAgent(rabbitmq_vhost="mls.batch.pending_validation")

    def process_csv_file(self, filepath):
        """
        Process CSV file and publish rows to dedicated queue

        Args:
            filepath: Path to CSV file

        Returns:
            Dict with processing results and statistics
        """
        filename = os.path.basename(filepath)

        if not self.queue_agent:
            logger.error("Queue agent is not initialized.")
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
            self.queue_agent.create_queue(queue_name)

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
                self.queue_agent.publish_message(queue_name, json.dumps(message))
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

            logger.info(
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
