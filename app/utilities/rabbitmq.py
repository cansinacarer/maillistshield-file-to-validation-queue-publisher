from app.config import (
    RABBITMQ_HOST,
    RABBITMQ_DEFAULT_VHOSTS,
    RABBITMQ_USERNAME,
    RABBITMQ_PASSWORD,
)
from app.utilities.logging import logger
import requests
import pika
import time
import json


class QueueAgent:
    """
    Agent to manage RabbitMQ queues and connections.

    For each vhost, create a different instance of this class.
    """

    def __init__(
        self,
        rabbitmq_vhost=RABBITMQ_DEFAULT_VHOSTS[0],
        rabbitmq_host=RABBITMQ_HOST,
        rabbitmq_port=5672,
        rabbitmq_username=RABBITMQ_USERNAME,
        rabbitmq_password=RABBITMQ_PASSWORD,
    ):
        self.rabbitmq_vhost = rabbitmq_vhost
        self.rabbitmq_host = rabbitmq_host
        self.rabbitmq_port = rabbitmq_port
        self.rabbitmq_username = rabbitmq_username
        self.rabbitmq_password = rabbitmq_password

        self.url = f"https://{self.rabbitmq_host}/api/queues/{self.rabbitmq_vhost.replace('/', '%2F')}"
        self.connection = None
        self.channel = None

        # Connect to RabbitMQ on initialization
        self.connect()

    def connect(self):
        """Connect to RabbitMQ via AMQP with retry logic"""
        max_retries = 5
        retry_delay = 5  # seconds

        for attempt in range(max_retries):
            try:
                credentials = pika.PlainCredentials(
                    self.rabbitmq_username, self.rabbitmq_password
                )
                parameters = pika.ConnectionParameters(
                    host=self.rabbitmq_host,
                    port=self.rabbitmq_port,
                    virtual_host=self.rabbitmq_vhost,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300,
                )
                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()

                # Only allow one unacknowledged message at a time
                self.channel.basic_qos(prefetch_count=1)

                logger.debug(
                    f"Connected to RabbitMQ at {self.rabbitmq_host}:{self.rabbitmq_port}/{self.rabbitmq_vhost}"
                )
                return True
            except Exception as e:
                logger.warning(
                    f"Connection attempt {attempt + 1}/{max_retries} failed: {e}"
                )
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        logger.error("Failed to connect to RabbitMQ after multiple attempts.")
        return False

    def disconnect(self):
        """Gracefully disconnect from RabbitMQ"""
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                logger.debug("Disconnected from RabbitMQ.")
        except Exception as e:
            logger.error(f"Error disconnecting from RabbitMQ: {e}")

    def list_all_queues_details(self):
        """
        List all queues in the RabbitMQ vhost specified for the parent.

        This connects to the RabbitMQ Management API to retrieve the list of queues.
        """

        try:
            response = requests.get(
                self.url,
                auth=requests.auth.HTTPBasicAuth(
                    self.rabbitmq_username, self.rabbitmq_password
                ),
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Error connecting to RabbitMQ Management API:\n{e}")
            return None

    def list_all_queues(self):
        """
        List all queues in the RabbitMQ vhost specified for the parent.
        """
        queues_details = self.list_all_queues_details()

        queue_names = [queue.get("name") for queue in queues_details]
        return queue_names

    def create_queue(self, queue_name, arguments={}):
        """
        Create a queue in RabbitMQ if it does not exist.
        """
        try:
            # Declare the queue (idempotent operation)
            self.channel.queue_declare(
                queue=queue_name, arguments=arguments, durable=True
            )
            logger.debug(f"Created queue: '{queue_name}'.")
            return True
        except Exception as e:
            logger.error(f"Error creating queue '{queue_name}': {e}")
            # Try to reconnect and get message again
            if self.connect():
                return self.create_queue(queue_name, arguments=arguments)
            else:
                logger.error("Reconnection attempt from create_queue() failed.")

        return False

    def delete_queue(self, queue_name):
        """
        Delete a queue in RabbitMQ if it exists.
        """
        try:
            self.channel.queue_delete(queue=queue_name)
            logger.debug(f"Deleted queue: '{queue_name}'.")
            return True
        except Exception as e:
            logger.error(f"Error deleting queue '{queue_name}': {e}")

            # Try to reconnect and delete again
            logger.warning(
                "Connection is closed. Attempting to reconnect and try deleting again."
            )
            if self.connect():
                self.delete_queue(queue_name)
            else:
                logger.error("Reconnection attempt from delete_queue() failed.")

        return False

    def publish_message(self, queue_name, message_body):
        """
        Publish a message to a specified queue.

        Args:
            queue_name: Name of the queue to publish to.
            message_body: The message body as a dict.

        Returns:
            True if the message was published successfully, False otherwise.
        """
        try:
            self.channel.basic_publish(
                exchange="",
                routing_key=queue_name,
                body=json.dumps(message_body),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                ),
            )
            logger.debug(
                f"Published message to vhost '{self.rabbitmq_vhost}', queue '{queue_name}'."
            )
            return True
        except Exception as e:
            logger.error(f"Error publishing message to queue '{queue_name}': {e}")

            # Try to reconnect and get message again
            logger.warning(
                "Connection is closed. Attempting to reconnect and try getting message again."
            )
            if self.connect():
                return self.publish_message(queue_name, message_body)
            else:
                logger.error("Reconnection attempt from publish_message() failed.")

        return False

    def get_message_count(self, queue_name, message_type="ready"):
        """
        Get the number of messages in a specified queue.

        We need to connect to the RabbitMQ Management API for this, because
        the AMQP protocol does not provide a way to get the unacked message count.
        """

        try:
            response = requests.get(
                f"{self.url}/{queue_name}",
                auth=requests.auth.HTTPBasicAuth(
                    self.rabbitmq_username, self.rabbitmq_password
                ),
            )
            response.raise_for_status()

            data = response.json()
        except Exception as e:
            logger.error(f"Failed to get message count: {e}")
            return None

        match message_type:
            case "unacked":
                # In progress, picked up by a consumer but not yet acked
                return data.get("messages_unacknowledged", 0)
            case "ready":
                # Ready to be delivered to a consumer
                return data.get("messages_ready", 0)
            case "total":
                # Total no of messages in the queue
                return data.get("messages", 0)
            case _:
                logger.error(f"Invalid message_type '{message_type}' specified.")
                return None

    def get_message_counts(self, queue_name):
        """
        Get the counts of ready, unacked, and total messages in a specified queue.

        Returns:
            A dict with keys 'ready', 'unacked', and 'total'.
        """
        return {
            "ready": self.get_message_count(queue_name, message_type="ready"),
            "unacked": self.get_message_count(queue_name, message_type="unacked"),
            "total": self.get_message_count(queue_name, message_type="total"),
        }

    def get_message(self, queue_name, auto_ack=False):
        """
        Retrieve a single message from the specified queue.

        Args:
            queue_name: Name of the queue to retrieve from.
            auto_ack: Whether to automatically acknowledge the message.

        Returns:
            The message body as a dict if a message is available, None otherwise.
        """
        try:
            method_frame, properties, body = self.channel.basic_get(
                queue=queue_name, auto_ack=auto_ack
            )
            if method_frame:
                logger.debug(
                    f"Retrieved message from vhost '{self.rabbitmq_vhost}', queue '{queue_name}'."
                )
                message = json.loads(body)
                # Append the delivery_tag for ack/nack operations
                message["delivery_tag"] = method_frame.delivery_tag
                return message
            else:
                logger.debug(f"No messages in queue '{queue_name}'.")
                return None
        except Exception as e:
            logger.error(f"Error retrieving message from queue '{queue_name}': {e}")

            # Try to reconnect and get message again
            logger.warning(
                "Connection is closed. Attempting to reconnect and try getting message again."
            )
            if self.connect():
                return self.get_message(queue_name, auto_ack=auto_ack)
            else:
                logger.error("Reconnection attempt from get_message() failed.")

        return None

    def retrieve_all_messages_and_delete_queue(self, queue_name):
        """
        Retrieve all messages from the specified queue.

        This is used to drain the queue when generating result files.

        We have a timeout of 5 minutes to avoid infinite loops.

        Args:
            queue_name: Name of the queue to retrieve from.

        Returns:
            A list of message bodies as dicts.
        """
        messages_retrieved = []
        start_time = time.time()

        # While messages remain in the queue and elapsed time is less than 5 minutes, keep fetching
        while (
            self.get_message_count(queue_name) > len(messages_retrieved)
            and (time.time() - start_time) < 300
        ):
            message = self.get_message(queue_name, auto_ack=True)
            if message:
                messages_retrieved.append(message)
            else:
                logger.error(
                    f"Expected more messages in queue '{queue_name}' but failed to retrieve."
                )

        logger.debug(
            f"Retrieved all {len(messages_retrieved)} messages from queue {queue_name}."
        )
        return messages_retrieved

    def acknowledge_message(self, message):
        """
        Acknowledge a message by its delivery tag.

        Args:
            message: The message body dict with the delivery tag appended.

        Returns:
            True if the message was acknowledged successfully, False otherwise.
        """
        try:
            # Grab the delivery tag we appended to the message dict
            delivery_tag = message.get("delivery_tag")
            if not delivery_tag:
                logger.error("Message does not contain a delivery_tag.")
                return False

            self.channel.basic_ack(delivery_tag)
            logger.debug(f"Acknowledged message with delivery tag '{delivery_tag}'.")
            return True
        except Exception as e:
            logger.error(
                f"Error acknowledging message with delivery tag '{delivery_tag}': {e}"
            )

            # Try to reconnect and get message again
            logger.warning(
                "Connection is closed. Attempting to reconnect and try getting message again."
            )
            if self.connect():
                return self.acknowledge_message(message)
            else:
                logger.error("Reconnection attempt from acknowledge_message() failed.")
        return False

    def reject_message(self, message, requeue=True):
        """
        Reject a message by its delivery tag.

        Args:
            message: The message body dict with the delivery tag appended.
            requeue: Whether to requeue the message.

        Returns:
            True if the message was rejected successfully, False otherwise.
        """
        try:
            # Grab the delivery tag we appended to the message dict
            delivery_tag = message.get("delivery_tag")
            if not delivery_tag:
                logger.error("Message does not contain a delivery_tag.")
                return False

            # Reject the message
            self.channel.basic_nack(delivery_tag, requeue=requeue)
            logger.debug(
                f"Rejected message with delivery tag '{delivery_tag}'. Requeue: {requeue}"
            )
            return True
        except Exception as e:
            logger.error(
                f"Error rejecting message with delivery tag '{delivery_tag}': {e}"
            )

            # Try to reconnect and get message again
            logger.warning(
                "Connection is closed. Attempting to reconnect and try getting message again."
            )
            if self.connect():
                return self.reject_message(message, requeue=requeue)
            else:
                logger.error("Reconnection attempt from reject_message() failed.")

        return False

    def get_queue_props(self, queue_name):
        """
        List all attributes of a queue in the RabbitMQ vhost specified for the parent.

        This connects to the RabbitMQ Management API to retrieve the list of queues.
        """

        try:
            response = requests.get(
                f"{self.url}/{queue_name}",
                auth=requests.auth.HTTPBasicAuth(
                    self.rabbitmq_username, self.rabbitmq_password
                ),
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Error connecting to RabbitMQ Management API:\n{e}")
            return None

    def get_expected_message_count(self, queue_name):
        """
        Get the expected message count from the queue arguments.

        Returns:
            The expected message count as an integer, or None if not set.
        """
        props = self.get_queue_props(queue_name)
        if not props:
            return None

        arguments = props.get("arguments", {})
        return arguments.get("row_count")

    def get_job_uid(self, queue_name):
        """
        Get the value of the jobuid argument of a queue.

        This is set when the queue is created for a specific job.
        After a file is processed, it is then passed to the results queue
        when that is created for a file.
        """
        props = self.get_queue_props(queue_name)
        if not props:
            return None

        arguments = props.get("arguments", {})
        return arguments.get("jobuid")
