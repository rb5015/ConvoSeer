import os
import time
import uuid
from typing import Iterator, Dict, Any
import click
from kafka import KafkaProducer
import orjson
from tqdm import tqdm


def _default(value: Any) -> Any:
    if isinstance(value, (set,)):
        return list(value)
    raise TypeError


def json_dumps(data: Dict[str, Any]) -> bytes:
    return orjson.dumps(data, default=_default)


def load_jsonl(path: str) -> Iterator[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield orjson.loads(line)


@click.command()
@click.option("--input", "-i", type=click.Path(exists=True, dir_okay=False), default="datasets/prepared/utterances.jsonl", show_default=True)
@click.option("--topic", "-t", default=os.getenv("KAFKA_TOPIC_RAW", "calls.raw"), show_default=True)
@click.option("--brokers", "-b", default=os.getenv("KAFKA_BROKERS", "localhost:9092"), show_default=True)
@click.option("--rate", "-r", type=float, default=5.0, help="Utterances per second", show_default=True)
@click.option("--max", "max_msgs", type=int, default=0, help="Max messages to send (0 = all)", show_default=True)
def main(input: str, topic: str, brokers: str, rate: float, max_msgs: int) -> None:
    """Replay prepared utterances to Kafka topic as raw stream."""
    producer = KafkaProducer(bootstrap_servers=brokers, value_serializer=json_dumps, key_serializer=lambda x: x.encode("utf-8"))
    interval = 1.0 / max(rate, 0.001)
    count = 0

    for rec in tqdm(load_jsonl(input), desc="Producing"):
        call_id = rec.get("call_id") or str(uuid.uuid4())
        key = call_id
        producer.send(topic, key=key, value=rec)
        count += 1
        if max_msgs and count >= max_msgs:
            break
        time.sleep(interval)

    producer.flush()
    producer.close()
    print(f"Sent {count} messages to {topic}")


if __name__ == "__main__":
    main()


