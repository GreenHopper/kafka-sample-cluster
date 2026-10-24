"""Sample record generation for the delivery-topic catalogue.

Pure functions: no Kafka imports. Every entity attribute is derived from the
entity's 1-based index plus the fixed seed, which gives the two properties the
seeder relies on:

* determinism — the same inputs produce identical records;
* index stability — re-deriving an index that was produced earlier returns the
  values produced back then, so an append can reference old records without
  reading them back from the broker.
"""

import datetime as dt
import math
import random

import catalog

UTC = dt.timezone.utc

FIRST_NAMES = [
    "Ada", "Bo", "Cara", "Dan", "Elif", "Farid", "Greta", "Hugo", "Ida",
    "Jonas", "Kira", "Lena", "Milo", "Nora", "Omar", "Petra", "Quinn",
    "Rosa", "Sven", "Tara",
]
LAST_NAMES = [
    "Ahlberg", "Bergmann", "Costa", "Dahl", "Ekert", "Fontaine", "Gruber",
    "Hansen", "Iversen", "Jansen", "Keller", "Lindqvist", "Moreau", "Nagy",
    "Osten", "Petrov", "Quaresma", "Roth", "Sander", "Vogel",
]
STREET_NAMES = [
    "Maple Row", "Oakgate Street", "Kiefernweg", "Kirchplatz", "Quay Lane",
    "Kanalstrasse", "Weidegrund", "Bergblick", "Ringweg", "Marktallee",
    "Fichtenhain", "Torbogen", "Werftweg", "Gartenhof", "Muehlrain",
    "Sonnhalde",
]
COMPANY_SUFFIXES = ["Foods", "Market", "Studio", "Parts", "Goods", "Bakery"]
VEHICLE_TYPES = ["VAN", "TRUCK", "TRAILER", "CARGO_BIKE"]
EMISSION_CLASSES = ["EURO5", "EURO6", "ELECTRIC"]
LICENCE_CLASSES = ["B", "C", "C+E", "D1"]
TIME_WINDOWS = ["MORNING", "AFTERNOON", "EVENING"]
LOYALTY_TIERS = ["BRONZE", "SILVER", "GOLD"]
PRIORITIES = ["STANDARD", "EXPRESS", "SAME_DAY"]
HANDLING_FLAGS = ["FRAGILE", "REFRIGERATED", "HAZARDOUS", "UPSIDE_DOWN",
                  "STACKABLE"]
ITEM_ADJECTIVES = ["organic", "dry", "frozen", "bulk", "boxed", "loose",
                   "premium", "refill", "seasonal", "assorted"]
ITEM_NOUNS = ["flour", "coffee beans", "apples", "motor oil", "cables",
              "paper stock", "yogurt", "seeds", "cleaning fluid", "bottles",
              "packaging", "tools", "herbs", "spare filters"]
ACTOR_TYPES = ["SYSTEM", "DISPATCHER", "DRIVER", "CUSTOMER"]
FIX_QUALITIES = ["GPS", "A-GPS", "SIMULATED"]
COUNTRY_CODES = ["DE", "AT", "CH", "NL", "BE"]
EMAIL_DOMAIN = "sample-corp.test"  # RFC 2606: never deliverable

_START = dt.date.fromisoformat(catalog.ROUTE_START_DATE)
_HOUR_MS = 3_600_000
_KM_PER_DEG_LAT = 111.32

# Reference ranges for one run. They depend only on the run's counts, so an
# index re-derived in a later run resolves to the same record.
# shortcut: appended records reference the identifier range of the base run
# rather than the whole id space; store references per index if that is ever
# required.
def reference_limits(counts=None):
    base = catalog.entity_counts(counts)
    return {
        "depot": (1, catalog.DEPOT_COUNT),
        "delivery_address": (
            catalog.DEPOT_COUNT + catalog.PICKUP_COUNT + 1,
            base["addresses"],
        ),
        "drivers": (1, base["drivers"]),
        "vehicles": (1, base["vehicles"]),
        "customers": (1, base["customers"]),
        "routes": (1, base["routes"]),
    }


def _rng(kind, index):
    return random.Random(f"{catalog.SEED}:{kind}:{index}")


def _pick(kind, index, limits):
    low, high = limits[kind]
    return random.Random(f"{catalog.SEED}:ref:{kind}:{index}").randint(low,
                                                                      high)


def _iso_date(day_offset):
    return (_START + dt.timedelta(days=day_offset)).isoformat()


def _epoch_ms(day_offset, hour):
    day_start = dt.datetime(_START.year, _START.month, _START.day, tzinfo=UTC)
    return int((day_start + dt.timedelta(days=day_offset,
                                         hours=hour)).timestamp() * 1000)


def _km_per_deg_lon(lat):
    return 111.32 * math.cos(math.radians(lat))


def _date_string(rng, first_year, last_year):
    return (f"20{rng.randint(first_year, last_year)}-"
            f"{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}")


def address(index):
    """Address record. The kind follows the index ranges in the catalogue."""
    rng = _rng("addresses", index)
    area_index = index % len(catalog.AREAS)
    area_name, lat0, lon0 = catalog.AREAS[area_index]
    if index <= catalog.DEPOT_COUNT:
        kind = "DEPOT"
    elif index <= catalog.DEPOT_COUNT + catalog.PICKUP_COUNT:
        kind = "PICKUP"
    else:
        kind = "DELIVERY"
    return {
        "address_id": catalog.make_id("adr", index),
        "kind": kind,
        "street": rng.choice(STREET_NAMES),
        "house_number": f"{rng.randint(1, 240)}{rng.choice(['', 'a', 'b'])}",
        "postal_code": f"{area_name[0]}{rng.randint(1000, 9999)}",
        "city": area_name,
        "region": f"Region-{area_index + 1}",
        "country_code": rng.choice(COUNTRY_CODES),
        "lat": round(lat0 + rng.uniform(-0.20, 0.20), 6),
        "lon": round(lon0 + rng.uniform(-0.20, 0.20), 6),
        "contact_name": f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
        "phone": f"+49 {rng.randint(200, 999)} {rng.randint(1000000, 9999999)}",
    }


def driver(index, limits):
    rng = _rng("drivers", index)
    start_hour = rng.choice([5, 6, 7, 8])
    return {
        "driver_id": catalog.make_id("drv", index),
        "first_name": rng.choice(FIRST_NAMES),
        "last_name": rng.choice(LAST_NAMES),
        "licence_class": rng.choice(LICENCE_CLASSES),
        "licence_expiry": _date_string(rng, 27, 29),
        "shift_start_hour": start_hour,
        "shift_end_hour": rng.choice(
            [h for h in (14, 15, 16, 17, 18, 22) if h > start_hour]
        ),
        "home_depot_id": catalog.make_id("adr", _pick("depot", index, limits)),
        "phone": f"+49 {rng.randint(150, 179)} {rng.randint(1000000, 9999999)}",
        "rating": round(rng.uniform(2.5, 5.0), 2),
        "hired_since": _date_string(rng, 15, 25),
    }


def vehicle(index, limits):
    rng = _rng("vehicles", index)
    kind = rng.choice(VEHICLE_TYPES)
    heavy = kind in ("TRUCK", "TRAILER")
    return {
        "vehicle_id": catalog.make_id("veh", index),
        "plate": (f"{''.join(rng.choices('ABCDEFGHJKLMSTVX', k=2))} "
                  f"{rng.randint(1, 9999):04d}"),
        "type": kind,
        "max_weight_kg": round(rng.uniform(8000, 26000) if heavy
                               else rng.uniform(300, 3500), 1),
        "max_volume_m3": round(rng.uniform(20, 90) if heavy
                               else rng.uniform(2, 20), 1),
        "emission_class": ("ELECTRIC" if kind == "CARGO_BIKE"
                           else rng.choice(EMISSION_CLASSES)),
        "home_depot_id": catalog.make_id("adr", _pick("depot", index, limits)),
        "last_service_date": _date_string(rng, 24, 26),
        "odometer_km": round(rng.uniform(500, 320000), 1),
    }


def customer(index, limits):
    rng = _rng("customers", index)
    return {
        "customer_id": catalog.make_id("cus", index),
        "name": f"{rng.choice(LAST_NAMES)} {rng.choice(COMPANY_SUFFIXES)}",
        "contact_email": f"contact{index}@{EMAIL_DOMAIN}",
        "phone": f"+49 {rng.randint(200, 999)} {rng.randint(100000, 999999)}",
        "address_id": catalog.make_id("adr", _pick("delivery_address", index, limits)),
        "preferred_time_window": rng.choice(TIME_WINDOWS),
        "since": _date_string(rng, 10, 25),
        "loyalty_tier": rng.choice(LOYALTY_TIERS),
    }


def route(index, limits):
    """Route plan. Stable across runs, so later stages can re-derive it."""
    rng = _rng("routes", index)
    depot = address(_pick("depot", index, limits))
    area_name, lat0, lon0 = catalog.AREAS[index % len(catalog.AREAS)]
    day = index % catalog.ROUTE_DAYS
    start_ts = _epoch_ms(day, rng.choice([5, 6, 7]))
    return {
        "route_id": catalog.make_id("rte", index),
        "route_date": _iso_date(day),
        "vehicle_id": catalog.make_id("veh", _pick("vehicles", index, limits)),
        "driver_id": catalog.make_id("drv", _pick("drivers", index, limits)),
        "depot_address_id": depot["address_id"],
        "start_ts": start_ts,
        "end_ts": start_ts + int(rng.uniform(4.0, 9.0) * _HOUR_MS),
        "planned_distance_km": round(rng.uniform(15.0, 260.0), 1),
        "area_name": area_name,
        "center_lat": round((depot["lat"] + lat0) / 2, 6),
        "center_lon": round((depot["lon"] + lon0) / 2, 6),
        "radius_km": round(rng.uniform(4.0, 22.0), 2),
    }


def is_cancelled(delivery_index):
    return delivery_index % catalog.CANCELLED_EVERY == 0


def _lifecycle(delivery_index):
    return (catalog.CANCELLED_LIFECYCLE if is_cancelled(delivery_index)
            else catalog.STATUS_LIFECYCLE)


def _event_times(delivery_index, plan):
    """Status/time pairs of a delivery, oldest first, ending inside the day."""
    rng = _rng("delivery-events", delivery_index)
    statuses = _lifecycle(delivery_index)
    count = len(statuses)
    created = plan["start_ts"] - rng.randint(12, 72) * _HOUR_MS
    last = min(plan["end_ts"],
               plan["start_ts"] + int(rng.uniform(1.5, 8.0) * _HOUR_MS))
    step = (last - created) // count
    return [(status, created + slot * step + rng.randint(0, 59) * 60_000)
            for slot, status in enumerate(statuses)]


def delivery(index, limits):
    rng = _rng("deliveries", index)
    plan = route(_pick("routes", index, limits), limits)
    times = _event_times(index, plan)
    weight = round(rng.uniform(0.4, 380.0), 2)
    promised_from = plan["start_ts"] + int(rng.uniform(0.5, 2.0) * _HOUR_MS)
    return {
        "delivery_id": catalog.make_id("dlv", index),
        "customer_id": catalog.make_id("cus", _pick("customers", index, limits)),
        "address_id": catalog.make_id("adr", _pick("delivery_address", index, limits)),
        "route_id": plan["route_id"],
        "vehicle_id": plan["vehicle_id"],
        "driver_id": plan["driver_id"],
        "status": times[-1][0],
        "created_ts": times[0][1],
        "promised_from_ts": promised_from,
        "promised_to_ts": promised_from + rng.choice([2, 3, 4]) * _HOUR_MS,
        "completed_ts": times[-1][1],
        "weight_kg": weight,
        "volume_m3": round(weight * rng.uniform(0.002, 0.02), 3),
        "total_price_cents": rng.randint(350, 240000),
        "priority": rng.choice(PRIORITIES),
    }


def delivery_item(delivery_index, slot, limits=None):
    index = (delivery_index - 1) * catalog.ITEMS_PER_DELIVERY + slot + 1
    rng = _rng("delivery-items", index)
    weight = round(rng.uniform(0.1, 120.0), 2)
    return {
        "item_id": catalog.make_id("itm", index),
        "delivery_id": catalog.make_id("dlv", delivery_index),
        "sku": f"SKU-{rng.randint(10000, 99999)}",
        "description": (f"{rng.choice(ITEM_ADJECTIVES)} "
                        f"{rng.choice(ITEM_NOUNS)}"),
        "quantity": rng.randint(1, 40),
        "weight_kg": weight,
        "length_cm": round(rng.uniform(5, 120), 1),
        "width_cm": round(rng.uniform(5, 80), 1),
        "height_cm": round(rng.uniform(2, 90), 1),
        "handling_flags": rng.sample(HANDLING_FLAGS, rng.randint(0, 2)),
        "declared_value_cents": rng.randint(100, 50000),
    }


def delivery_events(delivery_index, limits):
    """Events of one delivery as records, oldest first."""
    rng = _rng("delivery-event-attrs", delivery_index)
    plan = route(_pick("routes", delivery_index, limits), limits)
    base = catalog.events_before(delivery_index)
    records = []
    for slot, (status, ts) in enumerate(_event_times(delivery_index, plan)):
        index = base + slot + 1
        records.append({
            "event_id": catalog.make_id("evn", index),
            "delivery_id": catalog.make_id("dlv", delivery_index),
            "status": status,
            "ts": ts,
            "actor_type": rng.choice(ACTOR_TYPES),
            "note": f"{status.lower().replace('_', ' ')} recorded",
            "lat": round(plan["center_lat"] + rng.uniform(-0.02, 0.02), 6),
            "lon": round(plan["center_lon"] + rng.uniform(-0.02, 0.02), 6),
        })
    return records


def _path_point(plan, step):
    """Point of the route sweep for one fix index, inside the route radius."""
    fixes = catalog.POSITION_FIXES_PER_ROUTE
    angle = 2 * math.pi * (step % fixes) / fixes
    reach = plan["radius_km"] * (0.35 + 0.55 * abs(math.sin(angle * 1.7)))
    lat = plan["center_lat"] + reach * math.sin(angle) / _KM_PER_DEG_LAT
    lon = (plan["center_lon"]
           + reach * math.cos(angle) / _km_per_deg_lon(plan["center_lat"]))
    return lat, lon


def vehicle_position(route_index, step, limits):
    """One GPS fix of a route day. Independent of the other fixes, so seeding
    can resume at any fix index."""
    plan = route(route_index, limits)
    fixes = catalog.POSITION_FIXES_PER_ROUTE
    step_ms = (plan["end_ts"] - plan["start_ts"]) // (fixes + 1)
    rng = _rng("position", f"{route_index}:{step}")
    lat, lon = _path_point(plan, step)
    prev_lat, prev_lon = _path_point(plan, step - 1)
    lat = round(lat + rng.uniform(-0.002, 0.002), 6)
    lon = round(lon + rng.uniform(-0.002, 0.002), 6)
    heading = math.degrees(math.atan2(lon - prev_lon, lat - prev_lat)) % 360
    base_km = _rng("odometer-base", route_index).uniform(200.0, 1200.0)
    speed = round(rng.uniform(4.0, 78.0), 1)
    return {
        "vehicle_id": plan["vehicle_id"],
        "route_id": plan["route_id"],
        "ts": plan["start_ts"] + (step + 1) * step_ms,
        "lat": lat,
        "lon": lon,
        "speed_kmh": speed,
        "heading_deg": round(heading, 1),
        "odometer_km": round(base_km + speed * (step + 1) * step_ms
                             / _HOUR_MS, 1),
        "engine_on": speed > 0.5,
        "fix_quality": rng.choice(FIX_QUALITIES),
    }


def vehicle_positions(route_index, limits):
    """GPS fixes of one route day, oldest first."""
    return [vehicle_position(route_index, step, limits)
            for step in range(catalog.POSITION_FIXES_PER_ROUTE)]


def delivery_event(index, limits):
    """Event record of a global event index."""
    delivery_index = _delivery_index_of_event(index)
    events = delivery_events(delivery_index, limits)
    return events[index - catalog.events_before(delivery_index) - 1]


def _delivery_index_of_event(index):
    """Delivery that produced the event with the given global index."""
    low, high = 1, index
    while low < high:
        middle = (low + high) // 2
        if catalog.events_before(middle + 1) < index:
            low = middle + 1
        else:
            high = middle
    return low


def record_for(spec, index, limits):
    """Record of one topic at a 1-based record index.

    Index-addressable records let the seeder resume any topic at its current
    end offset, and let an append continue the identifier ranges.
    """
    entity = spec.entity
    if entity == "addresses":
        return address(index)
    if entity == "drivers":
        return driver(index, limits)
    if entity == "vehicles":
        return vehicle(index, limits)
    if entity == "customers":
        return customer(index, limits)
    if entity == "routes":
        return route(index, limits)
    if entity == "deliveries":
        return delivery(index, limits)
    if entity == "items":
        per = catalog.ITEMS_PER_DELIVERY
        return delivery_item((index - 1) // per + 1, (index - 1) % per, limits)
    if entity == "events":
        return delivery_event(index, limits)
    if entity == "positions":
        per = catalog.POSITION_FIXES_PER_ROUTE
        return vehicle_position((index - 1) // per + 1, (index - 1) % per,
                                limits)
    raise KeyError(f"no record builder for entity {entity}")


def topic_targets(counts=None, existing=None, append=True):
    """Total records per topic after the run, plus the counts already present.

    Seeding tops every topic up to the catalogue counts; appending adds one
    further catalogue batch on top of what the cluster already holds.
    """
    base = catalog.entity_counts(counts)
    present_master = {entity: (existing or {}).get(entity, 0) for entity in base}
    if append:
        master = {e: present_master[e] + base[e] for e in base}
    else:
        master = {e: max(present_master[e], base[e]) for e in base}
    derived = catalog.derived_targets(master)
    present_derived = catalog.derived_targets(present_master)
    totals = {}
    starts = {}
    for spec in catalog.CATALOGUE:
        if spec.entity in master:
            totals[spec.name] = master[spec.entity]
            starts[spec.name] = present_master[spec.entity]
        else:
            totals[spec.name] = derived[spec.entity]
            starts[spec.name] = present_derived[spec.entity]
    return totals, starts


def generate(counts=None, existing=None, append=True):
    """All records per topic for a run, built through `record_for`.

    `existing` maps entity name to the records already produced, so an append
    continues the identifier ranges and references records that are already on
    the broker.
    """
    limits = reference_limits(counts)
    totals, starts = topic_targets(counts, existing, append)
    records = {}
    for spec in catalog.CATALOGUE:
        records[spec.name] = [
            record_for(spec, index, limits)
            for index in range(starts[spec.name] + 1, totals[spec.name] + 1)
        ]
    return records
