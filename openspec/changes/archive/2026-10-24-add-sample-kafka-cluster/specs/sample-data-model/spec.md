# Spec Delta

## Purpose

Defines the reusable data contract of the sample cluster: which topics exist, how their keys and Avro value schemas are shaped, and which consistency rules the generated records follow.

## ADDED Requirements

### Requirement: Published topic catalogue

The cluster SHALL publish a fixed catalogue of nine delivery-domain topics: `delivery-drivers`, `delivery-vehicles`, `delivery-addresses`, `delivery-customers`, `delivery-routes`, `delivery-deliveries`, `delivery-delivery-items`, `delivery-delivery-events`, and `delivery-vehicle-positions`.

#### Scenario: Catalogue is complete

- **WHEN** the topic list of a seeded cluster is retrieved
- **THEN** all nine catalogue topics exist and each contains at least one record

#### Scenario: No undocumented topics

- **WHEN** the topic list of a seeded cluster is retrieved and internal topics are excluded
- **THEN** every remaining topic is part of the published catalogue

### Requirement: String record keys

Every record in every catalogue topic SHALL carry a key that is the UTF-8 encoding of a non-empty identifier string, and that string SHALL equal the identifier field of the record's Avro value.

#### Scenario: Key matches the value identifier

- **WHEN** a record is consumed from any catalogue topic and decoded
- **THEN** decoding the key as UTF-8 yields exactly the value's identifier field

#### Scenario: Key is not Avro encoded

- **WHEN** a record key is read as raw bytes
- **THEN** it carries no Schema Registry framing bytes and parses directly as text

### Requirement: Avro value schemas in the Schema Registry

Every catalogue topic SHALL have one Avro value schema registered in the Schema Registry under the subject `<topic>-value`, and every value on that topic SHALL be written in the Confluent Avro format with the Schema Registry framing bytes.

#### Scenario: Value decodes against the registered schema

- **WHEN** a consumer fetches the registered schema for a topic and decodes a value from that topic
- **THEN** the decode succeeds and the record validates against that schema

#### Scenario: One subject per topic

- **WHEN** the list of registered subjects is retrieved from the Schema Registry
- **THEN** each catalogue topic contributes exactly one value subject and no key subject

### Requirement: Generic master-data entities

The catalogue SHALL model reusable delivery entities rather than application-specific ones: drivers with licence and shift attributes, vehicles with type and capacity, addresses typed as depot, pickup or delivery location, customers linked to an address, routes linking a vehicle and driver to a day, deliveries linking a customer and address to a route, delivery items belonging to a delivery, delivery events recording status changes, and vehicle positions recording GPS fixes.

#### Scenario: Address reuse across entities

- **WHEN** customers and deliveries are consumed
- **THEN** their address references resolve to records on the address topic, including depot addresses used by vehicles and routes

#### Scenario: Items belong to a delivery

- **WHEN** delivery items are consumed
- **THEN** each item references exactly one delivery and carries parcel attributes such as weight, dimensions and handling flags

### Requirement: Referential integrity of generated records

Every identifier referenced by a record SHALL be produced as the identifier of an existing record in the catalogue, within the same seeding run, and no reference SHALL be null unless its schema marks the field as optional.

#### Scenario: No dangling references

- **WHEN** every record of every catalogue topic is scanned and its references resolved
- **THEN** each reference resolves to exactly one record in the catalogue

#### Scenario: Position fixes belong to a tracked vehicle

- **WHEN** vehicle position records are consumed
- **THEN** each position references a vehicle that exists on the vehicle topic

### Requirement: Plausible timelines

Records that carry timestamps SHALL form monotonic and plausible sequences: a vehicle's positions SHALL be ordered within each route day, and a delivery's events SHALL progress through its status lifecycle without a later status preceding an earlier one.

#### Scenario: Status lifecycle ordered

- **WHEN** the events of one delivery are ordered by timestamp
- **THEN** the statuses follow the documented lifecycle order and end in a terminal status

#### Scenario: Position track follows a route day

- **WHEN** the positions of one vehicle on one route day are ordered by timestamp
- **THEN** the fixes are non-decreasing in time and stay within the area assigned to that route

### Requirement: Deterministic generation

For the same generator version and the same generation inputs, the generator SHALL produce identical identifier sets, field values, and per-topic record counts on every run.

#### Scenario: Two runs with the same inputs

- **WHEN** seeding is executed twice into two empty clusters with the same inputs
- **THEN** the decoded record sets of every topic are equal

#### Scenario: Append extends rather than repeats

- **WHEN** the append mode is used to add records to an already seeded cluster
- **THEN** the appended records introduce identifiers that do not collide with existing records

### Requirement: Minimum data volume

After seeding, every catalogue topic SHALL contain at least 1 000 records, and the time-oriented topics SHALL contain materially more records than the master-data topics.

#### Scenario: Volume floor met

- **WHEN** the end offset of each catalogue topic is read after seeding
- **THEN** every topic reports at least 1 000 records

#### Scenario: Telemetry dominates master data

- **WHEN** the per-topic record counts are compared
- **THEN** the position, event and item topics each hold more records than any master-data topic

### Requirement: Synthetic data only

All generated content SHALL be synthetic: names, addresses, and coordinates SHALL be drawn from built-in generator vocabulary and SHALL NOT reproduce real persons, real streets, or exported production data.

#### Scenario: No production data source

- **WHEN** the generator is run with no configuration supplied by the user
- **THEN** it produces records entirely from its built-in vocabulary and random seed
