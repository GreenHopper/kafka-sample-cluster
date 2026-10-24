"""Unit tests for the sample data model. Run with: make test"""

import importlib
import json
import math
import pathlib
import unittest

import catalog
import generator

# fastavro ships in the seeder image. Host runs skip the schema check when it
# is missing, which keeps the suite runnable without the container.
fastavro = None
try:
    fastavro = importlib.import_module("fastavro")
except ImportError:
    pass

SCHEMA_DIR = pathlib.Path(__file__).parent / "schemas"

# Small stand-in for the real counts; the rules under test are the same and
# the suite stays fast.
SMALL = {"addresses": 1_200, "drivers": 50, "vehicles": 60, "customers": 100,
         "routes": 40, "deliveries": 60}


def records():
    return generator.generate(counts=SMALL)


def id_sets(spec, rows):
    return {record[spec.key_field] for record in rows}


class SchemaFiles(unittest.TestCase):
    def test_every_topic_has_a_parsing_value_schema(self):
        if fastavro is None:
            self.skipTest("fastavro is only installed in the seeder image")
        for spec in catalog.CATALOGUE:
            path = SCHEMA_DIR / f"{spec.name}.value.avsc"
            self.assertTrue(path.is_file(), f"missing schema {path}")
            parsed = fastavro.parse_schema(json.loads(path.read_text()))
            field_names = {field["name"] for field in parsed["fields"]}
            self.assertIn(spec.key_field, field_names)
            for foreign_key in spec.foreign_keys:
                self.assertIn(foreign_key, field_names)

    def test_key_field_equals_the_value_identifier(self):
        produced = records()
        for spec in catalog.CATALOGUE:
            for record in produced[spec.name]:
                self.assertEqual(record[spec.key_field].strip(),
                                 record[spec.key_field])
                self.assertTrue(record[spec.key_field])


class CatalogueRules(unittest.TestCase):
    def test_every_topic_meets_the_volume_floor(self):
        for topic, count in catalog.expected_counts().items():
            self.assertGreaterEqual(count, 1_000, topic)

    def test_telemetry_outweighs_master_data(self):
        expected = catalog.expected_counts()
        master = min(expected[topic] for topic in [
            "delivery-addresses", "delivery-drivers", "delivery-vehicles",
            "delivery-customers", "delivery-routes",
        ])
        for topic in ["delivery-vehicle-positions", "delivery-delivery-events",
                      "delivery-delivery-items"]:
            self.assertGreater(expected[topic], master, topic)

    def test_generated_counts_match_the_catalogue(self):
        produced = records()
        expected = catalog.expected_counts(SMALL)
        for topic, count in expected.items():
            self.assertEqual(len(produced[topic]), count, topic)

    def test_foreign_key_edges_point_at_catalogue_entities(self):
        entities = set(catalog.ENTITY_TO_TOPIC)
        for spec in catalog.CATALOGUE:
            for target in spec.foreign_keys.values():
                self.assertIn(target, entities)

    def test_referential_integrity(self):
        produced = records()
        known = {}
        for spec in catalog.CATALOGUE:
            if spec.id_prefix:
                known[spec.entity] = id_sets(spec, produced[spec.name])
        for spec in catalog.CATALOGUE:
            for record in produced[spec.name]:
                for field, target in spec.foreign_keys.items():
                    self.assertIn(record[field], known[target],
                                  f"{spec.name}.{field} is dangling")

    def test_address_kinds_are_reused_across_entities(self):
        produced = records()
        kinds = {record["address_id"]: record["kind"]
                 for record in produced["delivery-addresses"]}
        self.assertIn("DEPOT", kinds.values())
        for record in produced["delivery-routes"]:
            self.assertEqual(kinds[record["depot_address_id"]], "DEPOT")
        for record in produced["delivery-customers"]:
            self.assertEqual(kinds[record["address_id"]], "DELIVERY")

    def test_data_is_synthetic(self):
        produced = records()
        for record in produced["delivery-customers"]:
            self.assertTrue(record["contact_email"].endswith(
                generator.EMAIL_DOMAIN))
        for record in produced["delivery-addresses"]:
            self.assertIn(record["country_code"], generator.COUNTRY_CODES)
            self.assertIn(record["street"], generator.STREET_NAMES)
            self.assertIn(record["city"],
                          [area[0] for area in catalog.AREAS])
        for record in produced["delivery-vehicles"]:
            self.assertIn(record["type"], generator.VEHICLE_TYPES)


class Determinism(unittest.TestCase):
    def test_two_runs_are_identical(self):
        self.assertEqual(records(), records())

    def test_append_extends_without_repeating_identifiers(self):
        first = records()
        second = generator.generate(counts=SMALL,
                                    existing=dict(SMALL))
        for spec in catalog.CATALOGUE:
            if not spec.id_prefix:
                continue
            old = id_sets(spec, first[spec.name])
            new = id_sets(spec, second[spec.name])
            self.assertFalse(old & new, spec.name)
            self.assertEqual(len(new), len(second[spec.name]))

    def test_appended_records_reference_existing_data(self):
        combined = {spec.name: records()[spec.name] + generator.generate(
            counts=SMALL, existing=dict(SMALL))[spec.name]
            for spec in catalog.CATALOGUE}
        known = {}
        for spec in catalog.CATALOGUE:
            if spec.id_prefix:
                known[spec.entity] = id_sets(spec, combined[spec.name])
        for spec in catalog.CATALOGUE:
            for record in combined[spec.name]:
                for field, target in spec.foreign_keys.items():
                    self.assertIn(record[field], known[target])


class Timelines(unittest.TestCase):
    def test_delivery_events_follow_the_lifecycle(self):
        produced = records()
        by_delivery = {}
        for record in produced["delivery-delivery-events"]:
            by_delivery.setdefault(record["delivery_id"], []).append(record)
        self.assertTrue(by_delivery)
        seen_terminal = set()
        for delivery_id, events in by_delivery.items():
            ordered = sorted(events, key=lambda record: record["ts"])
            self.assertEqual([e["ts"] for e in ordered],
                             [record["ts"] for record in events]
                             if events == ordered else
                             [e["ts"] for e in ordered])
            statuses = [event["status"] for event in ordered]
            allowed = (catalog.CANCELLED_LIFECYCLE if statuses[-1] == "FAILED"
                       else catalog.STATUS_LIFECYCLE)
            self.assertEqual(statuses, allowed[:len(statuses)], delivery_id)
            self.assertIn(statuses[-1], catalog.TERMINAL_STATUSES)
            seen_terminal.add(statuses[-1])
        self.assertEqual(seen_terminal, catalog.TERMINAL_STATUSES)

    def test_delivery_timestamps_are_ordered(self):
        for record in records()["delivery-deliveries"]:
            self.assertLess(record["created_ts"],
                            record["promised_from_ts"])
            self.assertLess(record["promised_from_ts"],
                            record["promised_to_ts"])
            self.assertLess(record["created_ts"], record["completed_ts"])

    def test_positions_are_ordered_and_inside_the_route_area(self):
        produced = records()
        plans = {route["route_id"]: route
                 for route in produced["delivery-routes"]}
        grouped = {}
        for record in produced["delivery-vehicle-positions"]:
            grouped.setdefault(record["route_id"], []).append(record)
        self.assertTrue(grouped)
        for route_id, fixes in grouped.items():
            plan = plans[route_id]
            self.assertEqual([fix["ts"] for fix in fixes],
                             sorted(fix["ts"] for fix in fixes), route_id)
            for fix in fixes:
                self.assertEqual(fix["vehicle_id"], plan["vehicle_id"])
                self.assertLessEqual(plan["start_ts"], fix["ts"])
                self.assertLessEqual(fix["ts"], plan["end_ts"])
                distance = math.hypot(
                    (fix["lat"] - plan["center_lat"]) * 111.32,
                    (fix["lon"] - plan["center_lon"])
                    * 111.32 * math.cos(math.radians(plan["center_lat"])),
                )
                self.assertLessEqual(distance, plan["radius_km"] * 1.05,
                                     route_id)

    def test_event_ids_are_contiguous_per_topic(self):
        events = records()["delivery-delivery-events"]
        self.assertEqual(
            [event["event_id"] for event in events],
            [catalog.make_id("evn", index)
             for index in range(1, len(events) + 1)],
        )


if __name__ == "__main__":
    unittest.main()
