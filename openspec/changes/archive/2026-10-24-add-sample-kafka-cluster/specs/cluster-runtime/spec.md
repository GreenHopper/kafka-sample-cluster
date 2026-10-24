# Spec Delta

## Purpose

Provides a disposable single-node Kafka test cluster with a Schema Registry whose start, stop, health, and data-durability behaviour downstream integration tests can depend on.

## ADDED Requirements

### Requirement: One-command cluster start

Starting the cluster SHALL require a single documented command and SHALL bring up a single-node Apache Kafka broker running in KRaft mode together with a Confluent Schema Registry, without any external coordination service.

#### Scenario: Cold start from an empty machine

- **WHEN** a user runs the documented start command on a machine with no prior cluster state
- **THEN** the broker reports an elected controller and an in-sync replica set for its own partitions
- **AND** the Schema Registry answers a topic request without a further manual step

#### Scenario: No ZooKeeper dependency

- **WHEN** the cluster is started from scratch
- **THEN** no ZooKeeper process, container, or connection string is required at any point

### Requirement: Published connection endpoints

The cluster SHALL expose the broker and the Schema Registry on fixed, documented host ports, and SHALL accept plaintext connections both from the host and from other containers on the cluster network.

#### Scenario: Producer running on the host

- **WHEN** an application on the host connects to the documented bootstrap address
- **THEN** it can list topics and produce records successfully

#### Scenario: Producer running in a container on the cluster network

- **WHEN** an application container joins the cluster network and connects to the documented in-network bootstrap address
- **THEN** it can list topics and produce records successfully

### Requirement: Data survives a stop and start cycle

Cluster data SHALL persist across a documented stop followed by the documented start, so that records produced before the stop are readable afterwards.

#### Scenario: Messages written before a restart

- **WHEN** records have been produced, the cluster is stopped with the documented stop command, and started again
- **THEN** the previously produced records are readable from the same topic with unchanged offsets

#### Scenario: Restart does not duplicate seed data

- **WHEN** the cluster is stopped and started again after it has already been seeded
- **THEN** the end offset of every seeded topic is unchanged

### Requirement: Destructive reset is explicit

Removing persisted cluster data SHALL require an explicitly destructive documented command, and the documentation SHALL state that this command deletes all messages and registered schemas.

#### Scenario: Reset requires the destructive command

- **WHEN** a user stops the cluster without the destructive flag
- **THEN** the persisted data remains available to the next start

#### Scenario: Reset deletes data

- **WHEN** a user runs the documented destructive reset
- **THEN** the next start behaves as a cold start with empty topics and no registered schemas

### Requirement: Cluster health is observable

The cluster SHALL provide a documented health check that reports whether the broker quorum is active and whether the Schema Registry is reachable, and it SHALL exit non-zero when either part is unhealthy.

#### Scenario: Healthy cluster

- **WHEN** the broker has an active controller and the Schema Registry answers requests
- **THEN** the health check reports both parts healthy and exits zero

#### Scenario: Registry down

- **WHEN** the Schema Registry is not reachable while the broker is up
- **THEN** the health check reports the registry as unhealthy and exits non-zero

### Requirement: Component versions are pinned

The cluster definition SHALL pin explicit image versions for the broker, the Schema Registry, and every supporting image, and the broker version SHALL be the newest stable Apache Kafka release line that the pinned Schema Registry supports.

#### Scenario: No floating tags

- **WHEN** the cluster definition is inspected
- **THEN** no image is referenced by a mutable tag such as `latest`

#### Scenario: Broker on the newest supported stable line

- **WHEN** the pinned versions are compared against the newest stable releases of the broker and the Schema Registry
- **THEN** the broker is on the newest stable major and minor line the pinned Schema Registry supports
- **AND** the pairing is recorded together with the reason for that choice
