import json
import random
import uuid
from datetime import datetime, timedelta


SENSOR_TYPE_TO_UNIT = {
    "humidity": "percentage",
    "temperature": "celsius",
    "motion": "boolean",
    "smoke": "boolean",
}

MANUFACTURERS = ["Bosch", "Honeywell", "Siemens", "Schneider"]

WAREHOUSES = [
    ("Rotterdam Hub", "NLD"),
    ("Hamburg Depot", "DEU"),
    ("London Station", "GBR"),
    ("Dallas Center", "USA"),
    ("Lyon Warehouse", "FRA"),
]

SECTION_LETTERS = ["A", "B", "C", "D"]
SECTIONS_PER_WAREHOUSE_RANGE = (2, 3)
UNCOVERED_SECTION_RATIO = 0.2


def build_sensors(count):
    rows = []
    for sid in range(1, count + 1):
        sensor_type = random.choice(list(SENSOR_TYPE_TO_UNIT))
        rows.append(
            (
                sid,
                sensor_type,
                random.choice(MANUFACTURERS),
                SENSOR_TYPE_TO_UNIT[sensor_type],
            )
        )
    return rows


def build_warehouses():
    return [(i + 1, name, country) for i, (name, country) in enumerate(WAREHOUSES)]


def build_sections(warehouses):
    sections = []
    section_id = 1
    for warehouse_id, *_ in warehouses:
        count = random.randint(*SECTIONS_PER_WAREHOUSE_RANGE)
        indices = set()
        while len(indices) < count:
            indices.add(f"{random.choice(SECTION_LETTERS)}{random.randint(1, 5)}")
        for idx in sorted(indices):
            sections.append((section_id, warehouse_id, idx))
            section_id += 1
    return sections


def build_installations(sensors, sections, ref_now):
    """
    A fraction of sections (UNCOVERED_SECTION_RATIO) is left without any
    installation, so coverage metrics are < 100%. The remaining sections each
    get one active installation with a distinct sensor; some of those sensors
    additionally get a historical, non-overlapping installation.
    """
    shuffled_sections = sections.copy()
    random.shuffle(shuffled_sections)
    uncovered_count = int(len(shuffled_sections) * UNCOVERED_SECTION_RATIO)
    covered_sections = shuffled_sections[uncovered_count:]

    assert len(sensors) >= len(
        covered_sections
    ), "need at least one sensor per covered section"

    sensor_ids = [sid for sid, *_ in sensors]
    random.shuffle(sensor_ids)

    installations = []
    inst_id = 1

    active_by_sensor = {}
    for section, sensor_id in zip(covered_sections, sensor_ids):
        section_id = section[0]
        start = ref_now - timedelta(days=random.randint(60, 400))
        installations.append((inst_id, section_id, sensor_id, start, None))
        active_by_sensor[sensor_id] = start
        inst_id += 1

    for sensor_id, active_start in list(active_by_sensor.items()):
        if random.random() < 0.3:
            first_end = active_start - timedelta(days=random.randint(1, 30))
            first_start = first_end - timedelta(days=random.randint(60, 200))
            prior_section_id = random.choice(sections)[0]
            installations.append(
                (inst_id, prior_section_id, sensor_id, first_start, first_end)
            )
            inst_id += 1

    return installations


def reading_value_json(sensor_type):
    if sensor_type == "humidity":
        return json.dumps(round(random.uniform(20, 90), 2))
    if sensor_type == "temperature":
        return json.dumps(round(random.uniform(-10, 40), 2))
    if sensor_type in ("motion", "smoke"):
        return json.dumps(random.random() < 0.05)
    raise ValueError(f"unknown sensor type: {sensor_type}")


def build_readings(sensors, installations, ref_now, per_installation):
    sensor_type_by_id = {sid: st for sid, st, *_ in sensors}
    readings = []
    for _, _, sid, start, end in installations:
        effective_end = end or ref_now
        window_seconds = int((effective_end - start).total_seconds())
        if window_seconds <= 0:
            continue
        sensor_type = sensor_type_by_id[sid]
        for _ in range(per_installation):
            ts = start + timedelta(seconds=random.randint(0, window_seconds))
            readings.append(
                (str(uuid.uuid4()), sid, ts, reading_value_json(sensor_type))
            )
    return readings
