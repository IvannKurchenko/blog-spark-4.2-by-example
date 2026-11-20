from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)


SENSOR_SCHEMA = StructType(
    [
        StructField("sensor_id", LongType(), nullable=False),
        StructField("sensor_type", StringType(), nullable=False),
        StructField("manufacturer", StringType(), nullable=False),
        StructField("measurement_unit", StringType(), nullable=False),
    ]
)

WAREHOUSE_SCHEMA = StructType(
    [
        StructField("warehouse_id", LongType(), nullable=False),
        StructField("warehouse_name", StringType(), nullable=False),
        StructField("warehouse_country", StringType(), nullable=False),
    ]
)

SECTION_SCHEMA = StructType(
    [
        StructField("section_id", LongType(), nullable=False),
        StructField("warehouse_id", LongType(), nullable=False),
        StructField("index", StringType(), nullable=False),
    ]
)

INSTALLATION_SCHEMA = StructType(
    [
        StructField("installation_id", LongType(), nullable=False),
        StructField("section_id", LongType(), nullable=False),
        StructField("sensor_id", LongType(), nullable=False),
        StructField("installation_timestamp", TimestampNTZType(), nullable=False),
        StructField("installation_removal", TimestampNTZType(), nullable=True),
    ]
)

# `value` is materialized as a JSON string here and promoted to VARIANT via
# parse_json when writing the table.
READING_RAW_SCHEMA = StructType(
    [
        StructField("reading_id", StringType(), nullable=False),
        StructField("sensor_id", LongType(), nullable=False),
        StructField("timestamp", TimestampNTZType(), nullable=False),
        StructField("value_raw", StringType(), nullable=False),
    ]
)
