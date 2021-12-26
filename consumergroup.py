"""
Start processing only latest records:
$ python consumergroup.py group1 c1
$ python consumergroup.py group1 c2
$ python consumergroup.py group1 c3

Start processing all records in the stream from the beginning:
$ python consumergroup.py group1 c1 --start-from 0
"""
import typer
import random
import time
from walrus import Database
from enum import Enum

BLOCK_TIME = 5000
STREAM_KEY = "app:event"

app = typer.Typer()


class StartFrom(str, Enum):
    beginning = "0"
    latest = "$"


@app.command()
def start(group_id, consumer_id: str, start_from: StartFrom = StartFrom.latest):
    rdb = Database()
    cg = rdb.consumer_group(group_id, [STREAM_KEY], consumer=consumer_id)
    cg.create()  # Create the consumer group. Default starts from the latest
    if start_from == StartFrom.beginning:
        cg.set_id(start_from)

    while True:
        print("Reading stream...")
        streams = cg.read(1, block=BLOCK_TIME)

        for stream_id, messages in streams:
            for message_id, message in messages:
                try:
                    print(f"processing {stream_id}::{message_id}::{message}")
                    print(float(message[b"temp"]), float(message[b"temp"]) > 0.5)
                    if float(message[b"temp"]) > 0.7:
                        # these messages will in pending state.
                        # https://redis.io/commands/XPENDING
                        # other consumers in the same group can claim with XCLAIM
                        raise ValueError("High temperature")
                    # simulate processing
                    time.sleep(random.randint(1, 3))
                    print(f"finished processing {message_id}")
                    cg.app_event.ack(message_id)
                    print(f"{cg.app_event.key} {cg.app_event.group}")
                except:
                    print(f"Error occured in processing {message_id}")
                    pass


if __name__ == "__main__":
    app()
