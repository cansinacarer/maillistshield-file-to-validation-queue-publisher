# Mail List Shield - File to Validation Queue Publisher

This microservice monitors the S3 bucket for cleaned, standardized files. When a file is found, its rows are queued in the RabbitMQ Queue. The queued files are moved to `validation/queued`.

__Job States:__

- Expected before:
  - `file_accepted`
- Error states
  - `error_?`
- Success state:
  - `file_queued`

See the [main repository](https://github.com/cansinacarer/maillistshield-com) for a complete list of other microservices.
