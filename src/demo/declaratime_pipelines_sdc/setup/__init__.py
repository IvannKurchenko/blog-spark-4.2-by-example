from pyspark.sql import SparkSession


def get_remote_spark() -> SparkSession:
    return (
        SparkSession.builder.remote("sc://localhost").appName("SimpleApp").getOrCreate()
    )
