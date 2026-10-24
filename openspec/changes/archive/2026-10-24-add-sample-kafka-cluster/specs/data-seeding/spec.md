# Spec Delta

## Purpose

Defines how the sample cluster fills itself with topic data: topic creation, schema registration, idempotent restart behaviour, explicit append of further records, and the read-back surface that proves seeding succeeded.

## ADDED Requirements

### Requirement: Topics are created with one partition

Seeding SHALL create every catalogue topic with exactly one partition and a replication factor of one, and SHALL leave the topic otherwise at broker defaults.

#### Scenario: Partition count after seeding

- **WHEN** the description of any catalogue topic is read after seeding
- **THEN** the topic reports exactly one partition

#### Scenario: Re-running seeding does not alter an existing topic

- **WHEN** seeding runs against a cluster where a catalogue topic already exists
- **THEN** the topic keeps its existing partition count and configuration

### Requirement: Automatic seeding after start

A freshly started, empty cluster SHALL become seeded by one documented command, and that command SHALL create the topics, register the schemas, and produce the records without further interaction.

#### Scenario: Seed a cold cluster

- **WHEN** the documented seed command is run against a started cluster that holds no data
- **THEN** every catalogue topic exists, every value schema is registered, and records are readable from every topic

#### Scenario: Seeding is idempotent with itself

- **WHEN** the seed command is interrupted and started again on the same cluster
- **THEN** the run completes and no record is duplicated within a topic identifier range already written

### Requirement: Seeding is skipped when data already exists

Seeding SHALL check each catalogue topic before producing and SHALL skip any topic that already holds records, reporting the skip instead of appending.

#### Scenario: Restarted cluster is not re-seeded

- **WHEN** the seed command runs against a cluster whose catalogue topics are already populated
- **THEN** no new records are produced and each topic is reported as skipped with its existing record count

#### Scenario: Only a missing topic is seeded

- **WHEN** a catalogue topic is absent or empty while the others are populated
- **THEN** only the missing or empty topic is created and populated

### Requirement: Additional records can be appended on demand

The system SHALL provide a documented append mode that produces one further batch of records into the catalogue while preserving the referential-integrity and timeline rules, so users can grow the data set deliberately.

#### Scenario: Append grows the data set

- **WHEN** the append mode is run against an already seeded cluster
- **THEN** end offsets increase on the appended topics and the new records resolve their references against records that exist

#### Scenario: Append is opt-in

- **WHEN** a user restarts the cluster and runs the plain seed command
- **THEN** no records beyond the ones already present are appended

### Requirement: Seeding reports its outcome

Seeding SHALL print, per catalogue topic, the action taken and the resulting record count, and SHALL exit zero only when every catalogue topic is seeded or reported as skipped.

#### Scenario: Summary on success

- **WHEN** seeding finishes successfully
- **THEN** the output lists every catalogue topic with its action and record count

#### Scenario: Summary reflects skipped topics

- **WHEN** seeding skips populated topics
- **THEN** those topics appear in the summary marked skipped with their existing counts

### Requirement: Seeding fails loudly

Seeding SHALL exit non-zero and state the cause when the broker or the Schema Registry is unreachable, when schema registration is rejected, or when production fails, and records already produced SHALL remain readable.

#### Scenario: Registry unreachable

- **WHEN** the seed command runs while the Schema Registry is not reachable
- **THEN** seeding stops with a non-zero exit and a message naming the registry endpoint

#### Scenario: Partial progress is retained

- **WHEN** seeding fails partway through a topic
- **THEN** the records already produced on that topic stay readable and the next run continues from the existing end offset

### Requirement: Seeded data is verifiable by read-back

The system SHALL provide a documented read-back command that reports the record count of every catalogue topic and prints one decoded example record per topic, so a user can confirm the seeded content without writing their own consumer.

#### Scenario: Read-back of a seeded cluster

- **WHEN** the read-back command runs against a seeded cluster
- **THEN** it prints one line per catalogue topic with a record count and one decoded example value

#### Scenario: Read-back of an unseeded cluster

- **WHEN** the read-back command runs against an empty cluster
- **THEN** it reports the missing topics and exits non-zero

### Requirement: Generated data stays out of version control

The repository SHALL contain only cluster definitions, schemas, generator code, and documentation; generated messages, Kafka log directories, and Schema Registry state SHALL NOT be committed.

#### Scenario: Seed run leaves the working tree clean

- **WHEN** seeding is executed and the repository status is inspected
- **THEN** no generated data file appears as an untracked change

#### Scenario: Reset does not require a source change

- **WHEN** a user discards all seeded data
- **THEN** no tracked file has to be deleted or edited to accomplish it
