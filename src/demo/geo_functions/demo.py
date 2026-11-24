from src.spark_provider import get_spark


# POLYGON ((4.450000 51.880000, 4.451453 51.880000, 4.451453 51.880539,
#           4.450000 51.880539, 4.450000 51.880000))
BUILD_WAREHOUSE_SQL = """
CREATE TABLE warehouse_geography
USING PARQUET
AS
SELECT 1L AS id,
       'Rotterdam Warehouse' AS name,
       'Netherlands' AS country,
       ST_GeogFromWKB(X'01030000000100000005000000CDCCCCCCCCCC1140713D0AD7A3F049406FB9FAB149CE1140713D0AD7A3F049406FB9FAB149CE114020ED7F80B5F04940CDCCCCCCCCCC114020ED7F80B5F04940CDCCCCCCCCCC1140713D0AD7A3F04940') AS polygon
"""

SHOW_WAREHOUSE_SQL = """
SELECT id, name, country, hex(ST_AsBinary(polygon)) AS polygon_wkb
FROM warehouse_geography
"""


def main():
    spark = get_spark()
    try:
        spark.sql(BUILD_WAREHOUSE_SQL)

        spark.table("warehouse_geography").printSchema()
        spark.sql(SHOW_WAREHOUSE_SQL).show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
