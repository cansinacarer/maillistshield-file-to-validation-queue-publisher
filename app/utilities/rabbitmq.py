from app.config import (
    RABBITMQ_HOST,
    RABBITMQ_VHOST,
    RABBITMQ_USERNAME,
    RABBITMQ_PASSWORD,
)
import requests
import pika


def list_all_queues(vhost="%2F"):
    """
    List all queues in the specified RabbitMQ vhost that start with the given prefix.
    """
    url = f"https://{RABBITMQ_HOST}/api/queues/{vhost}"

    try:
        response = requests.get(
            url, auth=requests.auth.HTTPBasicAuth(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to RabbitMQ Management API:\n{e}")
        return None

    return response.json()
