import sys
import random
from walrus import Database


def main(stream_key, sensor_id):
    rdb = Database()
    stream = rdb.Stream(stream_key)

    msg_id = stream.add(
        {
            "sensor_id": sensor_id,
            "temp": random.random(),
            "humidity": random.random(),
        },
        id="*"
    )
    print(f"message {msg_id} sent")


if __name__ == "__main__":
    stream_key = "app:event"
    sensor = sys.argv[1]
    main(stream_key, sensor)
