# Topic catalogue

## Run the cluster

```bash
make up       # start broker + schema registry, then seed whatever is missing
make down     # stop the containers, the data volume keeps every record
make seed     # seed only missing or partially seeded topics
make append   # add one further catalogue batch with fresh identifier ranges
make verify   # health check plus per-topic counts and one decoded record
make logs     # follow broker and registry logs
make reset    # DESTRUCTIVE: docker compose down -v, deletes containers and data
make test     # data-model unit tests inside the seeder image
```

`make up` and `make reset` wrap `docker compose up -d` and
`docker compose down -v`. A reset leaves no topics and no schema subjects, so
the next `make up` behaves exactly like a cold start.

Nine topics of generic delivery-domain data. Every topic has 1 partition,
replication factor 1, a plain UTF-8 string key, and an Avro value registered
in Schema Registry under `<topic>-value`.

| Topic | Key (string) | Value record | Records per seed run |
| --- | --- | --- | --- |
| `delivery-addresses` | `address_id` | `AddressValue` | 5,000 |
| `delivery-drivers` | `driver_id` | `DriverValue` | 1,000 |
| `delivery-vehicles` | `vehicle_id` | `VehicleValue` | 1,200 |
| `delivery-customers` | `customer_id` | `CustomerValue` | 4,000 |
| `delivery-routes` | `route_id` | `RouteValue` | 3,000 |
| `delivery-deliveries` | `delivery_id` | `DeliveryValue` | 12,000 |
| `delivery-delivery-items` | `item_id` | `DeliveryItemValue` | 36,000 |
| `delivery-delivery-events` | `event_id` | `DeliveryEventValue` | 57,600 |
| `delivery-vehicle-positions` | `vehicle_id` | `VehiclePositionValue` | 120,000 |

Total per seed run: **229,800 records**. An `append` run writes the same
counts again on top of the existing data, with fresh identifier ranges.

## How the records relate

```
customers ─┐
addresses ─┼─> deliveries ──┬──> delivery-items   (3 per delivery)
routes ────┤                └──> delivery-events  (lifecycle, see below)
vehicles ──┼─> routes ────────> vehicle-positions (40 GPS fixes per route)
drivers ───┘
```

- `delivery-routes` references one `vehicle_id`, one `driver_id` and one depot
  `address_id`.
- `delivery-deliveries` references a `customer_id`, a delivery `address_id`, a
  `route_id`, and copies the `vehicle_id` / `driver_id` of that route.
- `delivery-delivery-items` and `delivery-delivery-events` reference a
  `delivery_id`.
- `delivery-vehicle-positions` references a `route_id` and its `vehicle_id`,
  and is keyed by vehicle so all fixes of one vehicle land on one partition.

## Identifier format

`<prefix>-<zero-padded index>`, for example `adr-0000042` or `evn-0057600`.
Prefixes: `adr`, `drv`, `veh`, `cus`, `rte`, `dlv`, `itm`, `evn`. Indices
start at 1 and never repeat within a topic; an append run continues the range
instead of restarting it.

## Derived counts

- delivery items: 3 per delivery
- delivery events: 5 per delivery, 3 for every 10th delivery (that one ends in
  `FAILED` instead of `DELIVERED`)
- vehicle positions: 40 fixes per route, timestamp ordered, inside the radius
  assigned to the route

## Value fields per topic

- `delivery-addresses` (AddressValue): address_id, kind, street, house_number, postal_code, city, region, country_code, lat, lon, contact_name, phone
- `delivery-drivers` (DriverValue): driver_id, first_name, last_name, licence_class, licence_expiry, shift_start_hour, shift_end_hour, home_depot_id, phone, rating, hired_since
- `delivery-vehicles` (VehicleValue): vehicle_id, plate, type, max_weight_kg, max_volume_m3, emission_class, home_depot_id, last_service_date, odometer_km
- `delivery-customers` (CustomerValue): customer_id, name, contact_email, phone, address_id, preferred_time_window, since, loyalty_tier
- `delivery-routes` (RouteValue): route_id, route_date, vehicle_id, driver_id, depot_address_id, start_ts, end_ts, planned_distance_km, area_name, center_lat, center_lon, radius_km
- `delivery-deliveries` (DeliveryValue): delivery_id, customer_id, address_id, route_id, vehicle_id, driver_id, status, created_ts, promised_from_ts, promised_to_ts, completed_ts, weight_kg, volume_m3, total_price_cents, priority
- `delivery-delivery-items` (DeliveryItemValue): item_id, delivery_id, sku, description, quantity, weight_kg, length_cm, width_cm, height_cm, handling_flags, declared_value_cents
- `delivery-delivery-events` (DeliveryEventValue): event_id, delivery_id, status, ts, actor_type, note, lat, lon
- `delivery-vehicle-positions` (VehiclePositionValue): vehicle_id, route_id, ts, lat, lon, speed_kmh, heading_deg, odometer_km, engine_on, fix_quality

Types are in `seed/schemas/<topic>.value.avsc`: ids and names are `string`,
counts and flags are `int` / `boolean`, measurements are `float`, and every
timestamp is an `int` holding epoch milliseconds.

## Delivery status lifecycle

Every delivery is written as a sequence of `delivery-delivery-events` records
in this order:

```
PLANNED -> ASSIGNED -> PICKED_UP -> OUT_FOR_DELIVERY -> DELIVERED
```

Every 10th delivery is cancelled and stops after two events:

```
PLANNED -> ASSIGNED -> FAILED
```

Both sequences end in a terminal status (`DELIVERED` or `FAILED`), and event
timestamps rise within a delivery. `delivery-deliveries.status` always equals
the status of that delivery's last event, and `delivery-deliveries.completed_ts`
is set only for terminal deliveries.

Schema Registry holds exactly nine subjects: the nine `<topic>-value` subjects
above. Keys are plain strings, so they are not registered.
