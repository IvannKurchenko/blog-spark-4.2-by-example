from sentence_transformers import SentenceTransformer

from src.spark_provider import get_spark


SENSORS = [
    ("Motion", "boolean", "Detects movement in a monitored area."),
    ("Smoke", "percent per meter", "Detects airborne smoke from a fire."),
    ("CO2", "ppm", "Measures carbon dioxide concentration in the air."),
    ("Temperature", "°C", "Measures ambient air temperature."),
    ("Humidity", "% RH", "Measures relative humidity in the air."),
]
QUERY = "fire sensor"

NEAREST_SENSORS_SQL = """
SELECT q.query, c.name, c.unit, c.description,
       'Cosine similarity' AS metric,
       vector_cosine_similarity(q.embedding, c.embedding) AS score
FROM sensors_query q
JOIN sensors_catalog c
EXACT NEAREST 1 BY SIMILARITY vector_cosine_similarity(q.embedding, c.embedding)
UNION ALL
SELECT q.query, c.name, c.unit, c.description,
       'Inner product' AS metric,
       vector_inner_product(q.embedding, c.embedding) AS score
FROM sensors_query q
JOIN sensors_catalog c
EXACT NEAREST 1 BY SIMILARITY vector_inner_product(q.embedding, c.embedding)
UNION ALL
SELECT q.query, c.name, c.unit, c.description,
       'Negated Euclidean distance' AS metric,
       -1 * vector_l2_distance(q.embedding, c.embedding) AS score
FROM sensors_query q
JOIN sensors_catalog c
EXACT NEAREST 1 BY SIMILARITY -1 * vector_l2_distance(q.embedding, c.embedding)
ORDER BY metric, score DESC, name
"""


def main():
    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    texts = [QUERY, *(description for _, _, description in SENSORS)]
    query_embedding, *sensor_embeddings = model.encode(texts).tolist()

    spark = get_spark()
    try:
        catalog = spark.createDataFrame(
            [
                (*sensor, embedding)
                for sensor, embedding in zip(SENSORS, sensor_embeddings, strict=True)
            ],
            "name STRING, unit STRING, description STRING, embedding ARRAY<FLOAT>",
        )
        query = spark.createDataFrame(
            [(QUERY, query_embedding)], "query STRING, embedding ARRAY<FLOAT>"
        )
        # Explicit paths let a fresh local Spark session overwrite the demo data.
        warehouse = spark.conf.get("spark.sql.warehouse.dir").rstrip("/")
        catalog.write.mode("overwrite").option(
            "path", f"{warehouse}/sensors_catalog"
        ).saveAsTable("sensors_catalog")

        spark.table("sensors_catalog").show()

        query.write.mode("overwrite").option(
            "path", f"{warehouse}/sensors_query"
        ).saveAsTable("sensors_query")
        spark.table("sensors_query").show()

        spark.sql(NEAREST_SENSORS_SQL).show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
