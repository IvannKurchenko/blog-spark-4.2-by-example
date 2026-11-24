from pyspark.sql import SparkSession
from src.demo.declaratime_pipelines_sdc.setup.data import WAREHOUSES_TABLE, create_warehouses


def main():
    spark = SparkSession.builder.getOrCreate()
    try:
        spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
        create_warehouses(spark)
        spark.table(WAREHOUSES_TABLE).show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
