# Sample Kafka cluster (single node, KRaft) with Schema Registry

A throwaway cluster for testing Kafka consumers and producers. One command
starts a Kafka broker in KRaft mode plus Confluent Schema Registry, and seeds
nine Avro delivery-domain topics with ~230,000 records. Data lives in a Docker
volume, so stopping the cluster keeps the data.

Requires Docker with Compose v2. Nothing else on the host.

## Start

```bash
make up      # broker + registry, then the seeder runs once automatically
```

First start takes about a minute (image pull plus Java startup). After that the
seeder exits and the cluster is idle.

## Check what is there

```bash
make verify  # health: broker quorum + registry, then per-topic count and one record
```

## Add more data

```bash
make append  # writes another full catalogue batch with fresh identifier ranges
```

Re-running `make seed` is a no-op on a populated cluster: only missing or
partially seeded topics get written.

## Stop, restart, wipe

```bash
make down    # stop; volumes keep every record
make up      # same data, same offsets
make reset   # DESTRUCTIVE: removes containers and all data volumes
```

## Layout

| Path | Purpose |
| --- | --- |
| `compose.yaml` | broker, schema registry, one-shot seeder service |
| `seed/catalog.py` | topic list, record counts, identifier rules |
| `seed/generator.py` | deterministic record builders, one per topic index |
| `seed/schemas/` | one `.value.avsc` Avro schema per topic |
| `seed/seed_data.py` | creates topics, registers schemas, produces records |
| `scripts/healthcheck.sh` | broker quorum and registry reachability |
| `scripts/readback.py` | per-topic counts plus one decoded record |
| `docs/topics.md` | topic catalogue, keys, counts, relations |
| `docs/versions.md` | pinned image and package versions, why they were chosen |

## Topics

| Topic | Key | Records per seed run |
| --- | --- | --- |
| `delivery-addresses` | `address_id` | 5,000 |
| `delivery-drivers` | `driver_id` | 1,000 |
| `delivery-vehicles` | `vehicle_id` | 1,200 |
| `delivery-customers` | `customer_id` | 4,000 |
| `delivery-routes` | `route_id` | 3,000 |
| `delivery-deliveries` | `delivery_id` | 12,000 |
| `delivery-delivery-items` | `item_id` | 36,000 |
| `delivery-delivery-events` | `event_id` | 57,600 |
| `delivery-vehicle-positions` | `vehicle_id` | 120,000 |

Fields, foreign keys and the delivery status lifecycle: `docs/topics.md`.

## Data rules

- Every topic: 1 partition, string key, Avro value registered as
  `<topic>-value`.
- Keys are the record identifier (`delivery-vehicle-positions` is keyed by
  `vehicle_id`), which makes idempotent seeding checkable per identifier.
- Generation is deterministic: same catalogue counts and code produce the same
  identifiers and the same field values.
- No generated records are committed to git. Only schemas, scripts and docs.

## Change the shape of the data

Edit `seed/catalog.py` (counts) and, if needed, `seed/schemas/*.avsc`, then:

```bash
make reset && make up
```

For a schema change on a running cluster, wipe the volumes first: the registry
stores the old version and the seeder would otherwise be rejected as
incompatible.

## Tests

```bash
make test    # data-model unit tests inside the seeder image
```

Covers schema files parse, foreign keys resolve, identifiers are unique,
timestamps are ordered, and an append run continues the identifier ranges.

## Version bumps

Every image and package is pinned (see `docs/versions.md`). To bump a pin,
change the tag in `compose.yaml` or the version in `seed/requirements.txt`,
then verify the pairing on a wiped cluster:

```bash
make reset && make up && make verify && make test
```

Never move a pin to `:latest`; a registry and broker that disagree about
metadata show up as confusing seeder failures, not as a clear error.

## Ports

| Service | Host port | Override |
| --- | --- | --- |
| Kafka broker | 9092 | `KAFKA_HOST_PORT` in `.env` |
| Schema Registry | 8081 | `REGISTRY_HOST_PORT` in `.env` |

Copy `.env.example` to `.env` to change them. Inside the compose network the
addresses are `kafka:9093` and `schema-registry:8081`.
