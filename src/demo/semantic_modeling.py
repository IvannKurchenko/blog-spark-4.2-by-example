from pathlib import Path

from src.setup.create_setup import main as create_setup
from src.spark_provider import get_spark


SQL_DIR = Path(__file__).parent


def read_sql(name):
    return (SQL_DIR / name).read_text()


def main():
    create_setup()

    spark = get_spark()
    spark.sql(read_sql("installations_metrics.sql")).show()
    #spark.sql(read_sql("installations_metrics_all.sql")).show()
    #spark.sql(read_sql("installations_coverage_avg.sql")).show()
    #spark.sql(read_sql("installations_coverage_drill.sql")).show() - rolloup is not supporte
    spark.sql("""
        SELECT
            warehouse_country,
            warehouse_name,
            sensor_type,
            MEASURE(total_active_installed_sections) as total_active_installed_sections,
            MEASURE(total_sections) as total_sections,
            MEASURE(total_active_installed_sections) /  MEASURE(total_sections) as active_installations_coverage
        FROM installations_metrics
        GROUP BY ALL
        """).show()


if __name__ == "__main__":
    main()
