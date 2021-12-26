"""
Start processing only latest records:
$ python consumer.py consumer1 --start-from $
Start processing all records in the stream from the beginning:
$ python consumer.py consumer1 --start-from 0
"""
import typer
import random
import time
from walrus import Database
from enum import Enum


LAST_ID_KEY = "{consumer_id}:lastid"
BLOCK_TIME = 5000
STREAM_KEY = "app:event"

app = typer.Typer()


class StartFrom(str, Enum):
    beginning = "0"
    latest = "$"


@app.command()
def start(consumer_id: str, start_from: StartFrom = StartFrom.latest):
    rdb = Database()
    stream = rdb.Stream(STREAM_KEY)

    last_id = rdb.get(LAST_ID_KEY.format(consumer_id=consumer_id))
    if last_id:
        print(f"Resume from ID: {last_id}")
    else:
        last_id = start_from.value
        print(f"Starting from {start_from.name}")

    while True:
        print("Reading stream...")
        messages = stream.read(last_id=last_id, block=BLOCK_TIME)
        if messages:
            for message_id, message in messages:
                print(f"processing {message_id}::{message}")
                # simulate processing
                time.sleep(random.randint(1, 3))
                last_id = message_id
                rdb.set(LAST_ID_KEY.format(consumer_id=consumer_id), last_id)
                print(f"finished processing {message_id}")
        else:
            print(f"No new messages after ID: {last_id}")


if __name__ == "__main__":
    app()
