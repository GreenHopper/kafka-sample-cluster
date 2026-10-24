# Pinned versions

Every image and package is pinned to an exact version. Nothing in this repo
uses a `:latest` tag.

| Component | Pin | Released | Notes |
| --- | --- | --- | --- |
| Kafka broker image | `apache/kafka:4.4.0` | 2026-10-07 | KRaft only, no ZooKeeper. Latest stable Apache Kafka image. |
| Schema Registry image | `confluentinc/cp-schema-registry:8.3.2` | 2026-09-10 | Latest stable Confluent Platform registry. |
| Seeder base image | `python:3.13-slim` | 2026-10-21 (this build) | `3.13-slim` is the maintained slim line. |
| Kafka/registry client | `confluent-kafka[schemaregistry,avro]==2.16.0` | 2026-10-07 | librdkafka-backed, current PyPI release, ships manylinux wheels (no C compile). |
| Avro reference | `fastavro==1.13.1` | 2026-10-08 | Current PyPI release, used to validate the `.avsc` files in tests. |

## Broker and registry pairing

`apache/kafka:4.4.0` with `cp-schema-registry:8.3.2` is not a Confluent
certified combination, so it was verified before anything else was built on
top of it. Verified working:

- KRaft controller quorum elects a leader on a fresh volume.
- Schema Registry stores its `_schemas` topic on the broker and serves
  `GET /subjects`.
- All nine value schemas register, and Avro records produced through
  `confluent-kafka` decode back through the registry.

Fallback pin if a future upgrade breaks the pairing: `apache/kafka:4.3.2`.
Change it in `compose.yaml` and re-run `make up && make verify`.

## Why not PyKafka

`pykafka` last released 2.8.0 in 2018 and its repository is gone, which fails
the "actively maintained" rule. `confluent-kafka` is released continuously and
carries the Schema Registry Avro serializer that this cluster needs.

## Known image quirks

`cp-schema-registry:8.3.2` contains neither `curl` nor `wget`. Its Docker
health check therefore probes port 8081 with bash's `/dev/tcp` (see
`compose.yaml`). `scripts/healthcheck.sh` uses `curl` from the host instead.
