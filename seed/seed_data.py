"""Create the catalogue topics, register their Avro value schemas, and produce
the sample records.

Run inside the seeder container:

    docker compose run --rm --no-deps seeder python seed_data.py seed
    docker compose run --rm --no-deps seeder python seed_data.py append

Seeding tops every catalogue topic up to the counts in `catalog.py` and skips
topics that are already complete. Appending adds one further catalogue batch
on top of what the cluster holds.
"""
# The Kafka client packages are installed only in the seeder image, so the
# host interpreter cannot resolve them.
# pyright: reportMissingImports=false

import os
import pathlib
import sys

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaException
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

import catalog
import generator

SCHEMA_DIR = pathlib.Path(__file__).parent / "schemas"
BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
ADMIN_ID = "sample-cluster-seeder"


class SeedError(Exception):
    """Fatal seeding problem with a message that names the failing endpoint."""


def schema_text(topic):
    path = SCHEMA_DIR / f"{topic}.value.avsc"
    return path.read_text(encoding="utf-8")


def open_admin():
    return AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})


def open_consumer():
    return Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": f"{ADMIN_ID}-offsets",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })


def end_offsets(consumer):
    """End offset of partition 0 of every catalogue topic, 0 when missing."""
    offsets = {}
    for topic in catalog.TOPIC_NAMES:
        try:
            _, high = consumer.get_watermark_offsets(
                TopicPartition(topic, 0), timeout=15
            )
        except KafkaException:
            high = 0
        offsets[topic] = high
    return offsets


def create_missing_topics(admin, offsets):
    missing = [name for name, offset in offsets.items() if offset == 0
               and name not in admin.list_topics(timeout=15).topics]
    if not missing:
        return []
    futures = admin.create_topics([
        NewTopic(name, num_partitions=1, replication_factor=1)
        for name in missing
    ])
    for name, future in futures.items():
        try:
            future.result()
        except KafkaException as exc:
            if "already exists" not in str(exc).lower():
                raise SeedError(
                    f"cannot create topic {name} on broker "
                    f"{BOOTSTRAP_SERVERS}: {exc}"
                ) from exc
    return missing


def register_schemas(registry):
    """Register every value schema under <topic>-value and build serializers."""
    serializers = {}
    try:
        for spec in catalog.CATALOGUE:
            text = schema_text(spec.name)
            registry.register_schema(f"{spec.name}-value", Schema(text))
            serializers[spec.name] = AvroSerializer(registry, text)
    except Exception as exc:  # SchemaRegistryException and network failures
        raise SeedError(f"schema registry {REGISTRY_URL} rejected the "
                        f"catalogue schemas: {exc}") from exc
    return serializers


def value_context(topic):
    return SerializationContext(topic, MessageField.VALUE)


def produce_topic(producer, spec, serializer, start, total):
    """Produce records start+1..total of one topic. Returns the count."""
    errors = []

    def report(err, message):
        if err is not None:
            errors.append(str(err))

    limits = generator.reference_limits()
    for index in range(start + 1, total + 1):
        record = generator.record_for(spec, index, limits)
        producer.produce(
            topic=spec.name,
            key=str(record[spec.key_field]).encode("utf-8"),
            value=serializer(record, value_context(spec.name)),
            on_delivery=report,
        )
        if index % 2_000 == 0:
            producer.poll(0)
    producer.flush(300)
    if errors:
        raise SeedError(f"producing {spec.name} failed: {errors[0]}")
    return total - start


def run(append):
    try:
        admin = open_admin()
        consumer = open_consumer()
        offsets = end_offsets(consumer)
        created = create_missing_topics(admin, offsets)
        consumer.close()
    except KafkaException as exc:
        raise SeedError(f"broker {BOOTSTRAP_SERVERS} is unreachable: "
                        f"{exc}") from exc

    present = {spec.entity: offsets[spec.name] for spec in catalog.CATALOGUE
               if not spec.derived}
    totals, starts = generator.topic_targets(existing=present, append=append)
    for spec in catalog.CATALOGUE:
        if spec.derived:
            # A run that died mid-topic leaves more records than the master
            # counts imply; never write below the existing end offset.
            starts[spec.name] = max(starts[spec.name], offsets[spec.name])

    registry = SchemaRegistryClient({"url": REGISTRY_URL})
    serializers = register_schemas(registry)
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "enable.idempotence": True,
    })

    print(f"mode: {'append' if append else 'seed'}   "
          f"bootstrap: {BOOTSTRAP_SERVERS}   registry: {REGISTRY_URL}")
    if created:
        print("created topics: " + ", ".join(created))
    for spec in catalog.CATALOGUE:
        target = totals[spec.name]
        start = starts[spec.name]
        if target <= start:
            action, written = "skip", 0
        else:
            action = "append" if append else "seed"
            written = produce_topic(producer, spec, serializers[spec.name],
                                    start, target)
        print(f"{spec.name:28s} {action:6s} wrote {written:7d}  "
              f"total {start + written:7d}")
    return 0


def main(argv):
    mode = argv[1] if len(argv) > 1 else "seed"
    if mode not in ("seed", "append"):
        print("usage: seed_data.py [seed|append]", file=sys.stderr)
        return 2
    try:
        return run(append=(mode == "append"))
    except SeedError as exc:
        print(f"seeding failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
