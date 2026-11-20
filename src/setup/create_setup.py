import random
from datetime import datetime

from pyspark.sql import functions as F

from src.spark_provider import get_spark
from src.setup.data import (
    build_installations,
    build_readings,
    build_sections,
    build_sensors,
    build_warehouses,
)
from src.setup.schemas import (
    INSTALLATION_SCHEMA,
    READING_RAW_SCHEMA,
    SECTION_SCHEMA,
    SENSOR_SCHEMA,
    WAREHOUSE_SCHEMA,
)


SENSOR_COUNT = 20
READINGS_PER_INSTALLATION = 100
REF_NOW = datetime(2025, 11, 14, 12, 0, 0)
RANDOM_SEED = 42


def main():
    random.seed(RANDOM_SEED)
    spark = get_spark()

    sensors = build_sensors(SENSOR_COUNT)
    warehouses = build_warehouses()
    sections = build_sections(warehouses)
    installations = build_installations(sensors, sections, REF_NOW)
    readings = build_readings(sensors, installations, REF_NOW, READINGS_PER_INSTALLATION)

    def write(rows, schema, name):
        spark.createDataFrame(rows, schema=schema).write.mode("overwrite").saveAsTable(name)

    write(sensors, SENSOR_SCHEMA, "sensor")
    write(warehouses, WAREHOUSE_SCHEMA, "warehouse")
    write(sections, SECTION_SCHEMA, "section")
    write(installations, INSTALLATION_SCHEMA, "installation")

    (
        spark.createDataFrame(readings, schema=READING_RAW_SCHEMA)
        .withColumn("value", F.expr("parse_json(value_raw)"))
        .select("reading_id", "sensor_id", "timestamp", "value")
        .write.mode("overwrite")
        .saveAsTable("reading")
    )
