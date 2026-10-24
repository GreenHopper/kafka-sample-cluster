"""Topic catalogue for the sample cluster.

Pure data definitions: topic names, key fields, identifier scheme, record
counts, and the foreign-key edges that must always resolve. No Kafka imports,
so the generator and its unit tests run without a broker.
"""

from dataclasses import dataclass

# Fixed RNG seed: the generated data set is a pure function of this value and
# the counts below, so two runs give identical records.
SEED = 20261024

# Master-data record counts for one seeding run. Editing this dictionary is
# the documented way to change the data volume.
COUNTS = {
    "addresses": 5_000,
    "drivers": 1_000,
    "vehicles": 1_200,
    "customers": 4_000,
    "routes": 3_000,
    "deliveries": 12_000,
}

# Derived-record factors.
DEPOT_COUNT = 40
PICKUP_COUNT = 600
POSITION_FIXES_PER_ROUTE = 40
ITEMS_PER_DELIVERY = 3
# Every delivery gets PLANNED and ASSIGNED; every tenth delivery is cancelled
# (FAILED) and stops there, the rest run the full lifecycle.
CANCELLED_EVERY = 10
EVENTS_FULL_LIFECYCLE = 5
EVENTS_CANCELLED = 3

# Routes are spread over this many days starting from ROUTE_START_DATE.
ROUTE_DAYS = 60
ROUTE_START_DATE = "2026-06-01"

# Metro areas are invented, and their coordinates are arbitrary points chosen
# so that routes, tracks and radius checks behave like a real region.
AREAS = [
    ("Northport", 47.00, 8.00),
    ("Riverdale", 47.40, 8.50),
    ("Lakefield", 46.60, 8.20),
    ("Harborview", 47.20, 7.60),
    ("Westgate", 46.80, 9.00),
    ("Southvale", 46.40, 8.80),
    ("Greenhills", 47.60, 9.30),
    ("Eastbrook", 47.10, 9.80),
]


@dataclass(frozen=True)
class TopicSpec:
    name: str
    entity: str
    key_field: str
    # None means the record has no identifier of its own and is keyed by a
    # reference (vehicle positions are keyed by their vehicle id).
    id_prefix: str | None
    foreign_keys: dict
    derived: bool


# Generation order matters: every stage draws references only from entities
# that earlier stages define.
CATALOGUE = [
    TopicSpec(
        name="delivery-addresses",
        entity="addresses",
        key_field="address_id",
        id_prefix="adr",
        foreign_keys={},
        derived=False,
    ),
    TopicSpec(
        name="delivery-drivers",
        entity="drivers",
        key_field="driver_id",
        id_prefix="drv",
        foreign_keys={"home_depot_id": "addresses"},
        derived=False,
    ),
    TopicSpec(
        name="delivery-vehicles",
        entity="vehicles",
        key_field="vehicle_id",
        id_prefix="veh",
        foreign_keys={"home_depot_id": "addresses"},
        derived=False,
    ),
    TopicSpec(
        name="delivery-customers",
        entity="customers",
        key_field="customer_id",
        id_prefix="cus",
        foreign_keys={"address_id": "addresses"},
        derived=False,
    ),
    TopicSpec(
        name="delivery-routes",
        entity="routes",
        key_field="route_id",
        id_prefix="rte",
        foreign_keys={
            "vehicle_id": "vehicles",
            "driver_id": "drivers",
            "depot_address_id": "addresses",
        },
        derived=False,
    ),
    TopicSpec(
        name="delivery-deliveries",
        entity="deliveries",
        key_field="delivery_id",
        id_prefix="dlv",
        foreign_keys={
            "customer_id": "customers",
            "address_id": "addresses",
            "route_id": "routes",
            "vehicle_id": "vehicles",
            "driver_id": "drivers",
        },
        derived=False,
    ),
    TopicSpec(
        name="delivery-delivery-items",
        entity="items",
        key_field="item_id",
        id_prefix="itm",
        foreign_keys={"delivery_id": "deliveries"},
        derived=True,
    ),
    TopicSpec(
        name="delivery-delivery-events",
        entity="events",
        key_field="event_id",
        id_prefix="evn",
        foreign_keys={"delivery_id": "deliveries"},
        derived=True,
    ),
    TopicSpec(
        name="delivery-vehicle-positions",
        entity="positions",
        key_field="vehicle_id",
        id_prefix=None,
        foreign_keys={"vehicle_id": "vehicles", "route_id": "routes"},
        derived=True,
    ),
]

TOPICS = {spec.name: spec for spec in CATALOGUE}
TOPIC_NAMES = [spec.name for spec in CATALOGUE]
ENTITY_TO_TOPIC = {spec.entity: spec.name for spec in CATALOGUE}

# Documented lifecycle order. The last entry of a delivery's events is always
# one of the terminal statuses.
STATUS_LIFECYCLE = [
    "PLANNED",
    "ASSIGNED",
    "PICKED_UP",
    "OUT_FOR_DELIVERY",
    "DELIVERED",
]
CANCELLED_LIFECYCLE = ["PLANNED", "ASSIGNED", "FAILED"]
TERMINAL_STATUSES = {"DELIVERED", "FAILED"}


def entity_counts(counts=None):
    """Master-data counts for one run, with the catalogue defaults."""
    merged = dict(COUNTS)
    if counts:
        merged.update(counts)
    if merged["addresses"] <= DEPOT_COUNT + PICKUP_COUNT:
        raise ValueError(
            "addresses must exceed DEPOT_COUNT + PICKUP_COUNT so delivery "
            "addresses exist for customers and deliveries to reference"
        )
    return merged


def derived_counts(counts=None):
    """Record counts of the topics generated per delivery or per route."""
    base = entity_counts(counts)
    return derived_targets(base)


def events_before(delivery_index):
    """Event records produced for deliveries with a lower index.

    Every delivery runs the full lifecycle; every CANCELLED_EVERY-th delivery
    stops at FAILED, which is two events shorter.
    """
    previous = delivery_index - 1
    cancelled = previous // CANCELLED_EVERY
    full = previous - cancelled
    return (full * EVENTS_FULL_LIFECYCLE + cancelled * EVENTS_CANCELLED)


def derived_targets(master_targets):
    """Totals of the derived topics implied by master-topic totals."""
    deliveries = master_targets["deliveries"]
    return {
        "items": deliveries * ITEMS_PER_DELIVERY,
        "events": events_before(deliveries + 1),
        "positions": master_targets["routes"] * POSITION_FIXES_PER_ROUTE,
    }


def expected_counts(counts=None):
    """Total records per topic name for a run with the given master counts."""
    base = entity_counts(counts)
    derived = derived_counts(counts)
    totals = {spec.entity: base.get(spec.entity) or derived[spec.entity]
              for spec in CATALOGUE}
    return {spec.name: totals[spec.entity] for spec in CATALOGUE}


def make_id(prefix, index):
    """Stable identifier for an entity index (indices are 1-based)."""
    return f"{prefix}-{index:07d}"
