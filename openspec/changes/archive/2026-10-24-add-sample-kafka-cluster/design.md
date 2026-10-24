# Design

## Context

The repository is greenfield: an initial commit, a README, and OpenSpec scaffolding. Nothing exists to integrate with, so this design defines the whole layout. See `proposal.md` for motivation and the three spec deltas (`cluster-runtime`, `sample-data-model`, `data-seeding`) for the behaviour contract.

Constraints that shape the choices:

- Version currency is a hard requirement. Every image and package must be actively maintained, and the broker must sit on the newest stable Apache Kafka line. Verified against the registries on the day this change was planned:
  - `apache/kafka` on Docker Hub: newest stable tag `4.4.0` (release candidates excluded).
  - `confluentinc/cp-schema-registry` on Docker Hub: newest stable tag `8.3.2`.
  - `confluent-kafka` on PyPI: `2.16.0`, released this month, ships manylinux wheels for CPython 3.12/3.13 on x86_64 and aarch64.
  - `fastavro` on PyPI: `1.13.1`, released this month, supports CPython 3.11 through 3.15.
  - `pykafka` on PyPI: `2.8.0`, released 2018, classifiers stop at Python 3.6, and the upstream source repository returns 404. It is excluded by the maintenance rule (see Decisions).
- The host runs Python 3.14 and has no Kafka tooling installed; the seeder must not depend on host Python.
- Single node means replication factor 1 everywhere. Nothing in the design may assume more than one in-sync replica.

## Goals / Non-Goals

**Goals:**

- One Compose file is the entire cluster definition; one command per lifecycle action.
- Reproducible data: the same commit plus the same generator inputs produce byte-identical payloads, so two developers can diff their topic contents.
- The seeder must be safe to re-run at any time, including mid-failure, without duplicating records.
- Data written by the cluster never enters the git working tree.

**Non-Goals:**

- No multi-broker, no mirroring, no schema-registry authentication or TLS. Adding either is a separate change.
- No consumer groups, no ksqlDB, no Connect, no Kafka Streams examples. Downstream applications bring their own consumers.
- No data export to files (CSV/Parquet). Read-back happens against the broker.
- No automated dependency-bump pipeline. Version currency is enforced by a documented check at implementation and review time.

## Decisions

### D1. Broker: `apache/kafka:4.4.0`, single KRaft node, combined controller + broker

One container with `node.process.role=broker,controller` (the Kafka 4.x spelling of the former `process.roles`), a single node in both the controller and broker quorum, `num.partitions=1`, `default.replication.factor=1`, `offsets.topic.replication.factor=1`, `transaction.state.log.replication.factor=1` and `.min.isr=1`, and `auto.create.topics.enable=false`.

Two listeners: `PLAINTEXT://0.0.0.0:9093` for containers on the Compose network, `PLAINTEXT_HOST://0.0.0.0:9092` advertised to the host. `auto.create.topics.enable=false` keeps the topic list equal to the published catalogue — an application typo then fails loudly instead of silently creating a topic.

Why `apache/kafka` and not `confluentinc/cp-server`: the official Apache image is the upstream build, tracks the newest Kafka release directly, and avoids coupling the broker version to the Confluent Platform release train. Alternative rejected: `bitnami/kafka` (repackaged, historically lagging and its catalog moved to the Bitnami Secure Images catalog).

### D2. Schema Registry: `confluentinc/cp-schema-registry:8.3.2`, Kafka-backed, no external store

Kafka-backed storage keeps its state in the internal `_schemas` topic, so a single `kafka-data` volume covers durability for both components and there is no PostgreSQL container to keep alive. `KEY_CONVERTER` and `VALUE_CONVERTER` are set to `StringConverter` so the `_schemas` topic stays readable by the admin tooling. Compatibility level `BACKWARD` on all subjects. Plaintext on port 8081, no credentials.

The Confluent Schema Registry is the component the user asked for and the one every mainstream client already speaks. Alternative considered: `apicurio-registry` (Apache-2.0, no licence key) — rejected because the Confluent framing format the specs require is the registry-native one, and every consumer example downstream assumes it.

### D3. Version pairing is verified, not assumed

`8.3.2` is the newest Schema Registry; `4.4.0` is the newest broker. Confluent certifies its registry against the Kafka version bundled in its own platform release, so the newest/newest pair is expected to work but is not certified. The first implementation task boots exactly this pair, registers a schema, and produces one record. If the registry rejects the 4.4.0 broker, the fallback pin is `apache/kafka:4.3.2` with `cp-schema-registry:8.3.2`, and the README records the reason for the older broker line. This keeps the "newest stable" rule explicit and auditable instead of silently drifting.

### D4. Client library: `confluent-kafka[schemaregistry,avro]==2.16.0` with `fastavro`

The earlier sketch named `pykafka`. It cannot be used: its last release (2.8.0) is from 2018, it declares support only up to Python 3.6, and its source repository is gone. Under the maintenance rule that disqualifies it, and an unmaintained pure-Python client is also the riskiest component to talk to a Kafka 4.4 broker with.

`confluent-kafka` is actively released, is built on librdkafka (the reference client implementation for the Kafka protocol), and its `schemaregistry` and `avro` extras give the Confluent Avro format — magic byte plus schema id — out of the box. That removes hand-rolled framing, which was the largest correctness risk in the earlier sketch. Pinned install: `confluent-kafka[schemaregistry,avro]==2.16.0`, which pulls a compatible `fastavro` (its constraint is `>=1.5.4,<2`, satisfied by `1.13.1`); `fastavro` is additionally pinned directly so the schema-compilation path is explicit.

Alternatives rejected: `kafka-python==3.0.11` (maintained, but no Schema Registry serde, so the framing and subject handling would be hand-written) and a Java seeder (a JDK and a build for ~500 lines of Python's job).

### D5. The seeder runs in a container, not on the host

`seed/Dockerfile` based on `python:3.13-slim` with a pinned `seed/requirements.txt`, invoked as `docker compose run --rm seeder`. Python 3.13 is chosen because `confluent-kafka` ships prebuilt wheels for it; the host's 3.14 has no wheel, which would force a compiler on every machine. Containerised seeding also makes "same data on every machine" true regardless of host Python or OpenSSL.

### D6. Layout

```
compose.yaml            # kafka + schema-registry + seeder (profile-gated, not part of `up`)
.env.example            # host port overrides for 9092 / 8081
Makefile                # up | down | seed | append | verify | reset | logs
seed/
  Dockerfile
  requirements.txt
  seed_data.py          # CLI: seed | append | verify
  catalog.py            # topic table, record counts, id ranges, foreign keys
  generator.py          # pure data generation, no Kafka imports
  schemas/*.avsc        # one Avro value schema per topic
  test_generator.py     # pytest: determinism, referential integrity, timelines
scripts/healthcheck.sh  # broker quorum + registry reachability
scripts/readback.py     # per-topic count and one decoded record
docs/topics.md          # catalogue, schema fields, id scheme, volumes
```

Keeping `generator.py` free of Kafka imports is what makes the data rules testable without a broker.

### D7. Deterministic identity scheme

Every entity identifier is `<prefix>-<zero-padded sequence>` (`drv-000812`, `veh-000047`, `cus-0004194`, `adr-0008123`, `rte-0003012`, `dlv-00011943`, `itm-00039981`, `evn-00035991`). Positions carry no identifier of their own; their key is the vehicle id, which makes every GPS fix for one vehicle land in the topic's single partition in production order.

Generation order is master data first (addresses, drivers, vehicles, customers), then routes, then deliveries, then items and events, then positions. Each stage draws only from the identifier lists built by earlier stages, which is what makes the referential-integrity requirement provable by construction rather than by luck. One `random.Random(SEED)` instance with `SEED` a module constant drives all values, so the record set is a pure function of the catalogue constants.

### D8. Idempotence and append without a metadata topic

Before producing, the seeder reads each topic's end offset with `get_watermark_offsets`. Offset 0 means seed; offset greater than 0 means skip and report. Because each topic's identifiers were produced in sequence, a topic's end offset also bounds the identifiers already present, so append mode generates the identifier range `[end_offset, end_offset + batch)` and foreign keys are drawn from the union of old and new ranges enumerated by the same scheme. No `_seed-metadata` topic exists: it would violate the "no undocumented topics" scenario in `sample-data-model`.

`shortcut:` identifier ranges are inferred from end offsets, which assumes ids were written in sequence; if the seeder ever produces out of order or is given a compaction requirement, switch to an explicit per-run manifest file under `seed-state/`.

Producer settings: `enable.idempotence=True`, batching left to librdkafka defaults, one `flush()` per topic so a failure names the topic it failed on.

### D9. Volumes and the git boundary

Named volumes only: `kafka-data` for the log directory. Nothing is bind-mounted from the repository, so `docker compose down -v` is the only reset and `git status` stays clean after a seed run. `.gitignore` still lists `data/`, `seed-state/`, `__pycache__/`, and `.venv/` so an ad-hoc dump or a local experiment cannot be committed by accident.

### D10. Record volumes

Fixed constants in `catalog.py`: addresses 5 000, customers 4 000, drivers 800, vehicles 1 200, routes 3 000, deliveries 12 000, delivery items 40 000, delivery events 36 000, vehicle positions 120 000 — roughly 222 000 records, above the 1 000-per-topic floor for every topic and clearly telemetry-heavy. Estimated wire size is well under 100 MB on disk. Editing this one dictionary is the documented way to change the data volume; no environment-variable surface is added.

## Risks / Trade-offs

- [Newest broker and newest Schema Registry are not a Confluent-certified pairing] → D3 makes the boot of that exact pair the first implementation task, with a named fallback pin and a written reason in the docs.
- [`confluent-kafka` is a C-extension client; a platform without a matching wheel breaks the build] → the seeder runs in `python:3.13-slim`, where manylinux wheels for x86_64 and aarch64 exist, so no host compiler is needed.
- [222 000 Avro-encoded records can take longer than a developer will wait] → measure in the smoke task; if seeding exceeds five minutes, raise the producer batch size and lower per-record schema work by reusing one compiled schema object per topic before touching the record counts.
- [Schema Registry 8.x expects a licence key for production use and the image is under the Confluent Community License] → the README states local development and integration testing only; a production deployment is out of scope for this change.
- [Single-node, replication factor 1 means any volume loss destroys all data] → documented in the README next to the reset command; the reset command is deliberately separate from `down`.
- [Host port conflicts when the developer already runs Kafka or a registry on 9092/8081] → ports come from `.env` with those defaults, so a conflict is a one-line local override.
- [Deterministic ids make the data set look more "real" than it is] → all vocabulary is synthetic and the requirement in `sample-data-model` forbids production-derived content.

## Migration Plan

Greenfield: merge, `make up`, `make seed`. Rollback is `docker compose down -v` plus reverting the commit — no existing system or data is touched. Reverting the commit leaves the repository in its current state.
