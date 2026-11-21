
from pyspark.sql import SparkSession

from src.setup.data import build_warehouses
from src.setup.schemas import WAREHOUSE_SCHEMA


def main():
    spark = (
        SparkSession.builder.remote("sc://localhost")
        .appName("SimpleApp")
        .getOrCreate()
    )
    try:
        spark.sql("CREATE SCHEMA IF NOT EXISTS production.bronze")
        warehouses = spark.createDataFrame(build_warehouses(), schema=WAREHOUSE_SCHEMA)
        warehouses.writeTo("production.bronze.warehouses").using("delta").createOrReplace()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
