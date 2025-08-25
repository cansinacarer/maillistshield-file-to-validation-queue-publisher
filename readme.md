# Mail List Shield - Validation Queue Loader

This is the process the orchestrates the file validation. It reads the file in `in_progress` with the status `file_accepted`, reads them and sends them one by one to the workers.

When `row_count` == len(queue[file][results]):
- update status to `successful`,
- move the file to `validation/completed`

## ToDo

- If the container is stopped while there are files in queue in the memory, the files are lost. Maybe write the queued files in a txt?

## Batch Job States

- Expected before:
  - `file_accepted`
- Interim states
  - `queued`
  - `in_progress`
- Error states
  - `error_?`
- Success state:
  - `completed`

Process: Next job provider
Files queued list

Process: Validation executer

Process: Monitor progress
Process: Pack up the completed jobs
