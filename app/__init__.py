import time

from app.config import POLLING_INTERVAL, PAUSE
from app.uptime import ping_uptime_monitor
from app.utilities import list_files, enqueue_files, files_queued


def main():
    # Initialize the workers, which will continuously monitor the queue and consume

    # Main loop
    while True:
        # If Pause is active, skip everything
        if PAUSE:
            print(
                "The processing is paused, change the environment variable to continue."
            )
            time.sleep(5)
            continue

        # Iterations start time
        start_time = time.time()

        # List all files in validation/in-progress/
        all_files = list_files()

        # Filter for the ones we haven't seen before
        new_files = [file for file in all_files if file not in files_queued]

        # Enqueue the new files
        enqueue_files(new_files)

        print(f"files_queued: {[file['Key'] for file in files_queued]}")

        # TODO

        # Send a heartbeat to the uptime monitor
        print("Processing loop is active.")
        ping_uptime_monitor()

        # Iteration end time
        end_time = time.time()
        elapsed_time = end_time - start_time
        # If the elapsed time is not as long as the polling interval, sleep until it is
        sleep_time = POLLING_INTERVAL - elapsed_time
        if sleep_time > 0:
            time.sleep(sleep_time)
