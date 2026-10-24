# Tasks

## 1. Version pinning spike

- [x] 1.1 Pull `apache/kafka:4.4.0` and `confluentinc/cp-schema-registry:8.3.2`, boot both with a minimal KRaft single-node config, and verify the registry returns HTTP 200 on `/subjects` and the broker logs an active controller — this is the D3 pairing check from design.md.
- [x] 1.2 Register one throwaway Avro subject and produce one Confluent-Avro record against the 4.4.0 broker, and verify it consumes back with the schema id resolved — if this fails, apply the documented fallback pin `apache/kafka:4.3.2` and record the reason in `docs/topics.md` before continuing.
- [x] 1.3 Record the pinned image tags and Python package versions (`confluent-kafka[schemaregistry,avro]==2.16.0`, `fastavro==1.13.1`) plus their release dates in `docs/versions.md`, and verify no `:latest` tag appears in any file.

## 2. Cluster runtime

- [x] 2.1 Write `compose.yaml` with the single-node KRaft broker (combined `broker,controller` role, `num.partitions=1`, all replication factors 1, `auto.create.topics.enable=false`, `PLAINTEXT` on 9093 and `PLAINTEXT_HOST` on 9092) and verify `docker compose up -d kafka` leaves a healthy broker that reports one controller quorum member.
- [x] 2.2 Add the Schema Registry service (Kafka-backed `_schemas` storage, `StringConverter` key/value converters, `BACKWARD` compatibility, port 8081, `depends_on` broker health) and verify `docker compose up -d` reaches a state where `curl -fs localhost:8081/subjects` succeeds.
- [x] 2.3 Add the named volume `kafka-data` plus `.env.example` with `KAFKA_HOST_PORT=9092` and `REGISTRY_HOST_PORT=8081`, and verify a stop plus start keeps a topic that was written before the stop readable with unchanged offsets.
- [x] 2.4 Implement `scripts/healthcheck.sh` (broker controller quorum plus registry `/subjects` reachability, non-zero exit when either fails) and verify it exits 0 on a running cluster and non-zero after the registry container alone is stopped.
- [x] 2.5 Document start, stop, health, and the destructive `docker compose down -v` reset in `docs/topics.md` and verify every documented command is copy-pasteable and runs as written.

## 3. Schemas and data model

- [x] 3.1 Write the nine `.avsc` value schemas in `seed/schemas/` for drivers, vehicles, addresses, customers, routes, deliveries, delivery items, delivery events, and vehicle positions, and verify each parses with `fastavro.parse_schema`.
- [x] 3.2 Add `seed/catalog.py` with the topic table, per-topic record counts, id prefixes and ranges, and the declared foreign-key edges, and verify a unit test asserts every topic has at least 1 000 records and every foreign-key edge points at a topic in the catalogue.
- [x] 3.3 Implement `seed/generator.py` as pure functions (no Kafka imports) that build the entity lists in dependency order from one seeded `random.Random`, and verify unit tests cover determinism (two runs equal), referential integrity (no dangling reference in any topic), and the synthetic-vocabulary rule.
- [x] 3.4 Implement the timeline rules — positions ordered within a route day and inside the route area, delivery events ordered by the documented status lifecycle ending in a terminal status — and verify unit tests assert ordering for every generated delivery and every vehicle-day track.
- [x] 3.5 Document the catalogue, the id scheme, the per-topic field list, and the status lifecycle in `docs/topics.md` and verify the documented topic names and id prefixes match `catalog.py` exactly.

## 4. Seeding

- [x] 4.1 Write `seed/Dockerfile` (`python:3.13-slim`) and `seed/requirements.txt` with the pinned client packages, and verify `docker compose build seeder` installs without compiling the C extension.
- [x] 4.2 Implement topic creation through the admin client (one partition, replication factor 1, existing topics left untouched) and verify `docker compose run --rm seeder seed` on a fresh cluster yields nine topics each reporting exactly one partition.
- [x] 4.3 Implement schema registration under `<topic>-value` with the Confluent Avro serializer and verify the registry subject list equals the nine catalogue subjects with no key subjects.
- [x] 4.4 Implement production with an idempotent producer and per-topic flush, and verify `make seed` completes in under five minutes and each topic's end offset equals its configured record count.
- [x] 4.5 Implement the skip rule using end-offset checks per topic and verify a second `make seed` on a populated cluster produces zero new records and reports every topic as skipped with its existing count.
- [x] 4.6 Implement append mode that generates the identifier range starting at the current end offsets, and verify `make append` raises end offsets, introduces no id collision, and passes the referential-integrity check against the combined data set.
- [x] 4.7 Implement the per-topic action-plus-count summary and the non-zero exit paths for an unreachable broker or registry, and verify running the seeder with the registry stopped exits non-zero and names the registry endpoint.

## 5. Read-back and verification tooling

- [x] 5.1 Implement `scripts/readback.py` (per-topic record count and one decoded example value, non-zero exit when a catalogue topic is missing) and verify it prints nine lines against a seeded cluster and exits non-zero against an empty one.
- [x] 5.2 Add `make verify` wiring the health check and read-back together, and verify it fails before seeding and passes after seeding.

## 6. Repository hygiene

- [x] 6.1 Add `.gitignore` covering `data/`, `seed-state/`, `__pycache__/`, `.venv/` and verify `git status` is clean immediately after a full seed run.
- [x] 6.2 Add the `Makefile` targets `up`, `down`, `seed`, `append`, `verify`, `reset`, `logs` and verify each target runs from a clean checkout without any undocumented prerequisite.
- [x] 6.3 Rewrite `README.md` in English with the quickstart, the topic table, the durability and reset warning, and the maintenance/version-bump rule, and verify a first-time clone can go from `git clone` to a verified seeded cluster using only README steps.

## 7. Integration check

- [x] 7.1 Run the full cold path on a clean machine state — `make up`, `make seed`, `make verify` — and verify the nine topics are present with the configured counts and the read-back decodes all nine value schemas.
- [x] 7.2 Stop and restart the cluster and verify offsets are unchanged, the seed run reports all topics skipped, and records from before the restart consume back at their original offsets.
- [x] 7.3 Run `make reset` and verify the next start behaves as a cold start with no topics, no subjects, and a clean `git status`.
