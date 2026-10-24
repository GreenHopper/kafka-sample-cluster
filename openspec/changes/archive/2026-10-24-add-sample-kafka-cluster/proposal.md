# Proposal

## Why

Every Kafka application we build currently needs its own throw-away broker setup and its own fake data, so integration tests differ from machine to machine and nobody can reproduce another developer's result. This repository provides one reusable, disposable test cluster: a single-node Apache Kafka broker in KRaft mode plus a Confluent Schema Registry, automatically populated with internally consistent delivery-company sample data (drivers, vehicles, customers, GPS positions, deliveries, delivery events) that downstream applications can consume out of the box.

## What Changes

- New Docker Compose topology: single-node Kafka broker running in KRaft mode (controller + broker in one process, no ZooKeeper) and a Confluent Schema Registry, both backed by named Docker volumes so a `stop` / `start` cycle keeps every previously produced message readable.
- New topic catalogue: nine generic delivery-domain topics — master data (drivers, vehicles, addresses, customers), operations (routes, deliveries, delivery items), and telemetry (vehicle positions, delivery events) — each created with exactly **1 partition**, all using a **String key** and an **Avro value schema** registered in the Schema Registry.
- New seed-data generator: deterministic, referentially consistent records at large volume (at least 1 000 records in every topic, low six-figure total) produced on first start.
- New seeding lifecycle: seeding is skipped on restart when a topic already contains data, and an explicit append mode lets a user add another batch of records on demand.
- New repository layout and tooling: compose file, seed scripts, smoke-check script, `.gitignore` entries so generated data never lands in git, and English documentation covering the quickstart, topic catalogue, and schema list.
- Only scripts, schemas, and documentation are committed. Generated topic data lives exclusively in Docker volumes.
- Library policy for this change: every dependency must be actively maintained at the time of implementation, and versions are pinned to the newest release that is compatible with the pinned broker. This excludes the abandoned `pykafka` client (last release 2.18/2.8.0, 2018, source repository no longer available) in favour of a maintained client — see design.md.

## Capabilities

### New Capabilities

- `cluster-runtime`: Bringing the test cluster up and down — single-node KRaft broker plus Schema Registry, published endpoints, health/readiness, and durability of data across restarts.
- `sample-data-model`: The published data contract — the set of topics, the String key convention, the Avro value schema of every topic, and the referential-integrity and determinism rules the generated records must satisfy.
- `data-seeding`: The automatic population behaviour — topic creation with one partition, schema registration, idempotent skip on restart, explicit append of further records, and the verification surface that proves a topic was seeded.

### Modified Capabilities

None. This repository has no specs yet; all three capabilities are new.

## Impact

- New files at the repository root: `compose.yaml`, `.gitignore`, `Makefile`, `seed/` (schemas plus generator), `scripts/`, `docs/`. Existing `README.md` grows into the English quickstart.
- Runtime dependencies pulled as container images and Python packages (all pinned): Apache Kafka broker image (latest stable 4.x line), Confluent Platform Schema Registry image (latest 8.x patch), a slim Python runtime image for the seeder, and a maintained Kafka client with Schema Registry Avro serde plus a maintained Avro codec.
- No existing code, API, or data is affected — the repository is currently empty apart from the OpenSpec setup and the README.
- Licensing note: Confluent Schema Registry images are distributed under the Confluent Community License and Confluent Platform 8.x expects a licence key for production deployment; the intended use here is local development and integration testing only.
- Machine requirements: Docker with Compose v2+ and roughly 4 GB free memory for the broker and registry.
