from decouple import config
import pytz
import boto3


PAUSE = config("PAUSE", cast=bool, default=False)

# S3 bucket name
S3_BUCKET_NAME = config("S3_BUCKET_NAME")

# Polling interval (in seconds) for checking the S3 bucket for new files, pinging uptime monitor, etc.
POLLING_INTERVAL = config("POLLING_INTERVAL", cast=int)

# Uptime monitor address
UPTIME_MONITOR = config("UPTIME_MONITOR")

# Database connection
DATABASE_CONNECTION_STRING = config("DATABASE_CONNECTION_STRING")

# RabbitMQ connection
RABBITMQ_HOST = config("RABBITMQ_HOST")
RABBITMQ_DEFAULT_VHOSTS = config("RABBITMQ_DEFAULT_VHOSTS", default="/").split(",")
RABBITMQ_USERNAME = config("RABBITMQ_USERNAME")
RABBITMQ_PASSWORD = config("RABBITMQ_PASSWORD")

# Logging to Loki
LOKI_USER = config("LOKI_USER")
LOKI_PASSWORD = config("LOKI_PASSWORD")
LOKI_HOST = config("LOKI_HOST")
SERVICE_NAME = config("SERVICE_NAME")

# Timezone used in this app
appTimezoneStr = config("TIMEZONE")
appTimezone = pytz.timezone(appTimezoneStr)

# S3 object
S3_ENDPOINT = config("S3_ENDPOINT")
S3_KEY = config("S3_KEY")
S3_SECRET = config("S3_SECRET")
s3 = boto3.resource(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_KEY,
    aws_secret_access_key=S3_SECRET,
)
