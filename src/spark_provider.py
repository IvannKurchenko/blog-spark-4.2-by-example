from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """Return a Spark session. Stubbed to local mode for now."""
    return (
        SparkSession.builder.appName("awc-local")
        .master("local[*]")
        .getOrCreate()
    )
