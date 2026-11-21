import logging

from src.setup.create_setup import main as create_setup
from src.spark_provider import get_spark


logger = logging.getLogger(__name__)


INSTALLATIONS_METRICS_VIEW = """
CREATE OR REPLACE VIEW installations_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1

source: >
    SELECT
        sec.section_id,
        sec.warehouse_id,
        sec.`index` as section_index,
        i.installation_id,
        i.sensor_id,
        i.installation_timestamp,
        i.installation_removal,
        s.sensor_type,
        s.manufacturer,
        s.measurement_unit,
        w.warehouse_name,
        w.warehouse_country
    FROM section sec
    LEFT JOIN installation i ON i.section_id = sec.section_id
    LEFT JOIN sensor s ON s.sensor_id = i.sensor_id
    LEFT JOIN warehouse w ON w.warehouse_id = sec.warehouse_id

dimensions:
  - name: warehouse_country
    expr: warehouse_country
  - name: warehouse_name
    expr: warehouse_name
  - name: section_index
    expr: section_index
  - name: sensor_type
    expr: sensor_type

measures:
  - name: total_active_installed_sections
    expr: COUNT(DISTINCT installation_id) FILTER (WHERE installation_id IS NOT NULL AND installation_removal IS NULL)
  - name: total_sections
    expr: COUNT(DISTINCT section_id)
$$
"""

INSTALLATIONS_METRICS_VIEW_ALL = "SELECT * FROM installations_metrics"

INSTALLATIONS_METRICS_VIEW_ROLLOUP = """
SELECT
    warehouse_country,
    warehouse_name,
    sensor_type,
    MEASURE(total_active_installed_sections) as total_active_installed_sections,
    MEASURE(total_sections) as total_sections,
    MEASURE(total_active_installed_sections) /  MEASURE(total_sections) as active_installations_coverage
FROM installations_metrics
GROUP BY ROLLUP (warehouse_country, warehouse_name, sensor_type)
"""

INSTALLATIONS_METRICS_VIEW_GROUPING_SETS = """
SELECT
    warehouse_country,
    warehouse_name,
    MEASURE(total_active_installed_sections) as total_active_installed_sections,
    MEASURE(total_sections) as total_sections,
    MEASURE(total_active_installed_sections) /  MEASURE(total_sections) as active_installations_coverage
FROM installations_metrics
GROUP BY GROUPING SETS ((warehouse_country), (warehouse_country, warehouse_name))
"""

INSTALLATIONS_METRICS_VIEW_BY_ALL = """
SELECT
    warehouse_country,
    warehouse_name,
    section_index,
    sensor_type,
    MEASURE(total_active_installed_sections) as total_active_installed_sections,
    MEASURE(total_sections) as total_sections,
    MEASURE(total_active_installed_sections) /  MEASURE(total_sections) as active_installations_coverage
FROM installations_metrics
GROUP BY ALL
"""

INSTALLATIONS_METRICS_VIEW_BY_WAREHOUSE = """
SELECT
    warehouse_country,
    warehouse_name,
    MEASURE(total_active_installed_sections) AS total_active_installed_sections,
    MEASURE(total_sections) AS total_sections,
    MEASURE(total_active_installed_sections) / MEASURE(total_sections) AS active_installations_coverage
FROM installations_metrics
GROUP BY warehouse_country, warehouse_name
"""

INSTALLATIONS_VIEW = """
CREATE OR REPLACE VIEW installations_view AS
SELECT
    sec.section_id,
    sec.warehouse_id,
    sec.`index` as section_index,
    i.installation_id,
    i.sensor_id,
    i.installation_timestamp,
    i.installation_removal,
    s.sensor_type,
    s.manufacturer,
    s.measurement_unit,
    w.warehouse_name,
    w.warehouse_country
FROM section sec
LEFT JOIN installation i ON i.section_id = sec.section_id
LEFT JOIN sensor s ON s.sensor_id = i.sensor_id
LEFT JOIN warehouse w ON w.warehouse_id = sec.warehouse_id
"""

INSTALLATIONS_METRICS_BY_WAREHOUSE_SECTION_SENSOR = """
WITH section_sensor_counts AS (
    SELECT
        warehouse_country,
        warehouse_name,
        section_index,
        sensor_type,
        COUNT(DISTINCT CASE
            WHEN installation_id IS NOT NULL AND installation_removal IS NULL
            THEN installation_id
        END) AS total_active_installed_sections,
        COUNT(DISTINCT section_id) AS total_sections
    FROM installations_view
    GROUP BY warehouse_country, warehouse_name, section_index, sensor_type
)
SELECT *, total_active_installed_sections / total_sections AS active_installations_coverage
FROM section_sensor_counts
"""


INSTALLATIONS_METRICS_BY_WAREHOUSE = """
WITH warehouse_counts AS (
    SELECT
        warehouse_country,
        warehouse_name,
        COUNT(DISTINCT CASE
            WHEN installation_id IS NOT NULL AND installation_removal IS NULL
            THEN installation_id
        END) AS total_active_installed_sections,
        COUNT(DISTINCT section_id) AS total_sections
    FROM installations_view
    GROUP BY warehouse_country, warehouse_name
)
SELECT *, total_active_installed_sections / total_sections AS active_installations_coverage
FROM warehouse_counts
"""

INSTALLATIONS_AGGREGATED_VIEW = """
CREATE OR REPLACE VIEW installations_aggregated_view AS
SELECT
    warehouse_country,
    warehouse_name,
    sec.`index` as section_index,
    sensor_type,
    COUNT(DISTINCT CASE
        WHEN i.installation_id IS NOT NULL AND i.installation_removal IS NULL
        THEN i.installation_id
    END) AS total_active_installed_sections,
    COUNT(DISTINCT sec.section_id) AS total_sections
FROM section sec
LEFT JOIN installation i ON i.section_id = sec.section_id
LEFT JOIN sensor s ON s.sensor_id = i.sensor_id
LEFT JOIN warehouse w ON w.warehouse_id = sec.warehouse_id
GROUP BY warehouse_country, warehouse_name, section_index, sensor_type
"""

INSTALLATIONS_AGGREGATED_VIEW_BY_WAREHOUSE = """
SELECT
    warehouse_country,
    warehouse_name,
    SUM(total_active_installed_sections) AS total_active_installed_sections,
    SUM(total_sections) AS total_sections,
    SUM(total_active_installed_sections) / SUM(total_sections) AS active_installations_coverage
FROM installations_aggregated_view
GROUP BY warehouse_country, warehouse_name
"""

def safe_sql(spark, query):
    try:
        spark.sql(query).show(n=5)
    except Exception as exc:
        logger.error("Spark SQL failed with %s\nQuery:\n%s", exc, query.strip())


def main():
    create_setup()

    spark = get_spark()
    print("Installation metrics via regular view:")
    safe_sql(spark, INSTALLATIONS_VIEW)
    safe_sql(spark, INSTALLATIONS_METRICS_BY_WAREHOUSE_SECTION_SENSOR)
    safe_sql(spark, INSTALLATIONS_METRICS_BY_WAREHOUSE)

    print("Installation metrics via regular aggregated view:")
    safe_sql(spark, INSTALLATIONS_AGGREGATED_VIEW)
    safe_sql(spark, INSTALLATIONS_AGGREGATED_VIEW_BY_WAREHOUSE)

    print("Installation metrics via metrics view:")
    safe_sql(spark, INSTALLATIONS_METRICS_VIEW)
    safe_sql(spark, INSTALLATIONS_METRICS_VIEW_BY_ALL)
    safe_sql(spark, INSTALLATIONS_METRICS_VIEW_BY_WAREHOUSE)

    safe_sql(spark, INSTALLATIONS_METRICS_VIEW_ALL)  # Expected error: SELECT * on a metrics view is currently unsupported.
    safe_sql(spark, INSTALLATIONS_METRICS_VIEW_ROLLOUP)  # Expected error: ROLLUP on a metrics view is currently unsupported.
    safe_sql(spark, INSTALLATIONS_METRICS_VIEW_GROUPING_SETS)



if __name__ == "__main__":
    main()
