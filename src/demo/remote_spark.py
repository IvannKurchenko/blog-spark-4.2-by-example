from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder.remote("sc://localhost")
        .appName("SimpleApp")
        .getOrCreate()
    )
    try:
        spark.sql("CREATE SCHEMA IF NOT EXISTS unity.bronze")
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS unity.bronze.warehouses (
                warehouse_id BIGINT NOT NULL,
                warehouse_name STRING NOT NULL,
                warehouse_country STRING NOT NULL
            ) USING DELTA
            """
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
