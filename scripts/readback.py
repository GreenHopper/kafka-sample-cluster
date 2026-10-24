"""Read the seeded data back: record count and one decoded example per topic.

Runs inside the seeder image:

    docker compose run --rm --no-deps seeder python readback.py

Exits non-zero when a catalogue topic is missing or empty.
"""
# The Kafka client packages are installed only in the seeder image, so the
# host interpreter cannot resolve them.
# pyright: reportMissingImports=false

import json
import sys

import catalog
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.error import KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from seed_data import BOOTSTRAP_SERVERS, REGISTRY_URL, schema_text


def open_consumer():
    return Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "sample-cluster-readback",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })


def last_record(consumer, topic, high):
    consumer.assign([TopicPartition(topic, 0, high - 1)])
    for _ in range(20):
        message = consumer.poll(2.0)
        if message is None:
            continue
        if message.error() is not None:
            raise RuntimeError(f"{topic}: {message.error()}")
        return message
    raise RuntimeError(f"{topic}: no record readable at offset {high - 1}")


def main():
    registry = SchemaRegistryClient({"url": REGISTRY_URL})
    deserializers = {
        spec.name: AvroDeserializer(registry, schema_text(spec.name))
        for spec in catalog.CATALOGUE
    }
    consumer = open_consumer()
    missing = []
    print(f"bootstrap: {BOOTSTRAP_SERVERS}   registry: {REGISTRY_URL}")
    for spec in catalog.CATALOGUE:
        try:
            _, high = consumer.get_watermark_offsets(
                TopicPartition(spec.name, 0), timeout=15
            )
        except KafkaException:
            high = 0
        if high == 0:
            missing.append(spec.name)
            print(f"{spec.name:28s} {'MISSING':>7s}")
            continue
        message = last_record(consumer, spec.name, high)
        ctx = SerializationContext(spec.name, MessageField.VALUE)
        value = deserializers[spec.name](message.value(), ctx)
        example = json.dumps(value, default=str)
        if len(example) > 140:
            example = example[:137] + "..."
        print(f"{spec.name:28s} {high:7d}  "
              f"key={message.key().decode('utf-8')}  {example}")
    consumer.close()
    if missing:
        print("missing or empty topics: " + ", ".join(missing),
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
