from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType


WAREHOUSES_TABLE = "bronze.warehouses"
WAREHOUSES = [
    (1, "Rotterdam Hub", "NLD", True),
    (2, "Hamburg Depot", "DEU", True),
    (3, "London Station", "GBR", True),
    (4, "Dallas Center", "USA", True),
    (5, "Lyon Warehouse", "FRA", True),
]
WAREHOUSES_SCHEMA = StructType(
    [
        StructField("warehouse_id", LongType(), nullable=False),
        StructField("warehouse_name", StringType(), nullable=False),
        StructField("warehouse_country", StringType(), nullable=False),
        StructField("warehouse_active", BooleanType(), nullable=False),
    ]
)
WAREHOUSE_TO_DEACTIVATE = 1


def create_warehouses(spark: SparkSession) -> None:
    (
        spark.createDataFrame(WAREHOUSES, WAREHOUSES_SCHEMA)
        .withColumn("updated_at", F.current_timestamp())
        .writeTo(WAREHOUSES_TABLE)
        .using("delta")
        .createOrReplace()
    )


def update_warehouses(spark: SparkSession) -> None:
    spark.sql(
        f"""
        UPDATE {WAREHOUSES_TABLE}
        SET warehouse_active = false, updated_at = current_timestamp()
        WHERE warehouse_id = {WAREHOUSE_TO_DEACTIVATE} AND warehouse_active = true
        """
    )
