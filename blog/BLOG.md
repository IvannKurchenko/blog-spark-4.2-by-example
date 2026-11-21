# Spark 4.2 by example: Hands-on overvie of new features

### Introduction
Most of the media content dedicated to Spark 4.2 recently left some questions behind with regard to a practical aspect.
This is a gap that this post tries to cover. We will discover new Spark 4.2 major features with focus on hands-on practise.

### Setup
First, lets outline a project setup that will serve as foundation for further demos.

An example data project is pipeline for IoT system that tracks readings from a number of sensors across enterprise warehouses, that belongs to the same company - Abstract Warehouse Company (AWC).

Domain model is the following:
- `warehouse` - single warehouse.
- `section` - each warehouse is subdivided on a number of sections. `warehouse` has 1-to-many relation with `section`;
- `sensor` - installable IoT device. Sensors differ by type, such as humiditiy motion etc.
- `installation` - `sensor` installation event into particular `section`. `installation` has 1-to-1 relations between `sensor` and `section`;
- `reading` - single sensor reading value at a point of a time. `sensor` has 1-to-many relations with `reading`;

Schema tables that represents corresponding domain entities:
- `sensor` - a table describing sensor metadata. Properties: sensor_id (incremental), sensor_type (Enumeration: humidity, temperature, motion, smoke etc), manufacturer: (string), measurement_unit (Enumeration: percentage, celcious, boolean etc); 
- `reading` - a table describing a sensor reading value in moment of time: reading_id (UUID), sensor_id (FK), timestamp (TIMESTAMP NTZ), value (measurement value, type variant for flexibility);
- `warehouse` - a table describing an enterprise warehouse. Properties: warehouse_id (incremental), warehouse_name (string), warehouse_county (ISO-3166 alpha-3 code);
- `section` - a table describing an enterprise warehouse section. Properties: section_id (incremental PK), warehouse_id (FK), index (alpha-numerical representation on map, e.g. A1,B3)
- `installation` - a table describing an sensor sensor installation in warehouse. Properties: installation_id (incremental PK), section_id (FK), sensor_id (FK), , installation_timestamp (TIMESTAMP NTZ), installation_removal (TIMESTAMP NTZ, optional);

### Spark: Metrics and Semantic Modeling
Metrics views is another obejct type that is now available in Spark. The main problem it solves is to bring semantic layer closer to raw data to have it defined in a single place.
Lets consider this on an example for our AWC. 

The theoretical use case: the company goes though warehouses digitalisation process. The managment wants to have visibility of the progress. It was proposed a business metric to track - "Active Installations Coverage", showing percentage of warehouses sections with actively installed sensors. Metric should be calculated in the same way but at the different granularity for the different management levels: C-suite management wants to understand situation over entire company, while country and ware house managements at their respective levels.

Classical approach would to define precise calculations per use case as following. First, we would need to create a view consolidating all the data:
```sql
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
```

Having this in place, we can now proceed to calculation at the most granular level - by warehouse, section and sensor type for warehouse managers:
```sql
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
```

Super now to it shows result similar to the following:
```
+-----------------+--------------+-------------+-----------+-------------------------------+--------------+-----------------------------+
|warehouse_country|warehouse_name|section_index|sensor_type|total_active_installed_sections|total_sections|active_installations_coverage|
+-----------------+--------------+-------------+-----------+-------------------------------+--------------+-----------------------------+
|              NLD| Rotterdam Hub|           A2|     motion|                              0|             1|                          0.0|
|              DEU| Hamburg Depot|           A2|     motion|                              1|             1|                          1.0|
|              FRA|Lyon Warehouse|           A3|      smoke|                              1|             1|                          1.0|
|              NLD| Rotterdam Hub|           B1|      smoke|                              0|             1|                          0.0|
|              NLD| Rotterdam Hub|           A2|   humidity|                              1|             1|                          1.0|
+-----------------+--------------+-------------+-----------+-------------------------------+--------------+-----------------------------+
```

One requirement has been solved one more to go. Now we need to calculate same metrics for different granularity - per warehouse for country managers:
```sql
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
```

which produces reult similar to followings:
```
+-----------------+--------------+-------------------------------+--------------+-----------------------------+
|warehouse_country|warehouse_name|total_active_installed_sections|total_sections|active_installations_coverage|
+-----------------+--------------+-------------------------------+--------------+-----------------------------+
|              USA| Dallas Center|                              2|             2|                          1.0|
|              FRA|Lyon Warehouse|                              2|             3|           0.6666666666666666|
|              NLD| Rotterdam Hub|                              2|             3|           0.6666666666666666|
|              GBR|London Station|                              3|             3|                          1.0|
|              DEU| Hamburg Depot|                              3|             3|                          1.0|
+-----------------+--------------+-------------------------------+--------------+-----------------------------+
```

Here you already can spot the issue - `total_active_installed_sections` and `total_sections` calculation duplicates.

The first attempt to re-use this logic in one place could be in creating view with pre-aggregated calculates:
```sql
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
```

so then to use it the following way:
```sql
SELECT
    warehouse_country,
    warehouse_name,
    SUM(total_active_installed_sections) AS total_active_installed_sections,
    SUM(total_sections) AS total_sections,
    SUM(total_active_installed_sections) / SUM(total_sections) AS active_installations_coverage
FROM installations_aggregated_view
GROUP BY warehouse_country, warehouse_name
```

So as a result for the same data, we will get the following completely different numbers:
```
+-----------------+--------------+-------------------------------+--------------+-----------------------------+
|warehouse_country|warehouse_name|total_active_installed_sections|total_sections|active_installations_coverage|
+-----------------+--------------+-------------------------------+--------------+-----------------------------+
|              USA| Dallas Center|                              2|             3|           0.6666666666666666|
|              FRA|Lyon Warehouse|                              2|             3|           0.6666666666666666|
|              NLD| Rotterdam Hub|                              2|             5|                          0.4|
|              GBR|London Station|                              3|             4|                         0.75|
|              DEU| Hamburg Depot|                              3|             3|                          1.0|
+-----------------+--------------+-------------------------------+--------------+-----------------------------+
```

Its pretty easy to spot root cause of issue `SUM(total_sections) AS total_sections` is not the same as `total_sections` calculated per different aggregation group.
To keep it accurate in general case we still need to repeat same calculation. This is particularly where new metrics view shines and helps us.

Unlike regular view, metrics view are declared with YAML that separates notion of metrics and dimensions.
```sql
CREATE OR REPLACE VIEW installations_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1

# source query, view or table name to get data from.
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

# dimensions - used in `GROUP BY` and `SELECT` clauses
dimensions:
  - name: warehouse_country # alias
    expr: warehouse_country # source column or expression

  - name: warehouse_name
    expr: warehouse_name

  - name: section_index
    expr: section_index

  - name: sensor_type
    expr: sensor_type

# measures - used in `SELECT` clause, performs calculation over
measures:
  - name: total_active_installed_sections
    expr: COUNT(DISTINCT installation_id) FILTER (WHERE installation_id IS NOT NULL AND installation_removal IS NULL)

  - name: total_sections
    expr: COUNT(DISTINCT section_id)
$$
```

which is then can be used to calculate same metrics at the lowest granularity levels:
```
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
```

that produces result similar to the following: 
```
+-----------------+--------------+-------------+-----------+-------------------------------+--------------+-----------------------------+
|warehouse_country|warehouse_name|section_index|sensor_type|total_active_installed_sections|total_sections|active_installations_coverage|
+-----------------+--------------+-------------+-----------+-------------------------------+--------------+-----------------------------+
|              NLD| Rotterdam Hub|           A2|     motion|                              0|             1|                          0.0|
|              DEU| Hamburg Depot|           A2|     motion|                              1|             1|                          1.0|
|              FRA|Lyon Warehouse|           A3|      smoke|                              1|             1|                          1.0|
|              NLD| Rotterdam Hub|           B1|      smoke|                              0|             1|                          0.0|
|              NLD| Rotterdam Hub|           A2|   humidity|                              1|             1|                          1.0|
+-----------------+--------------+-------------+-----------+-------------------------------+--------------+-----------------------------+
```

And the use case where metrics view brings the most value - the very same metrics can be now recalculated with different granularity pretty easily:
```sql
SELECT
    warehouse_country,
    warehouse_name,
    MEASURE(total_active_installed_sections) AS total_active_installed_sections,
    MEASURE(total_sections) AS total_sections,
    MEASURE(total_active_installed_sections) / MEASURE(total_sections) AS active_installations_coverage
FROM installations_metrics
GROUP BY warehouse_country, warehouse_name
```

which produces desired numbers:
```
+-----------------+--------------+-------------------------------+--------------+-----------------------------+
|warehouse_country|warehouse_name|total_active_installed_sections|total_sections|active_installations_coverage|
+-----------------+--------------+-------------------------------+--------------+-----------------------------+
|              USA| Dallas Center|                              2|             2|                          1.0|
|              FRA|Lyon Warehouse|                              2|             3|           0.6666666666666666|
|              NLD| Rotterdam Hub|                              2|             3|           0.6666666666666666|
|              GBR|London Station|                              3|             3|                          1.0|
|              DEU| Hamburg Depot|                              3|             3|                          1.0|
+-----------------+--------------+-------------------------------+--------------+-----------------------------+
```

However, unline regular views metrics views have own limitation to keep in mind.
They can't be dirrectly queried. Plain `SELECT * FROM installations_metrics` results in error, so author has to explicitely specify dimensions, measurements and grouping.
Measures (`measures`) can't re-use other measures in definition.
In our particular case that would be great to introduce new measurement `active_installations_coverage` like:
```yaml
measures:
  - name: active_installations_coverage
    expr: ROUND(total_active_installed_sections / total_sections, 2)
```

However this is considered as invalid definition. The wourk around is repeated parts of query.

Last but not least, `GROUP BY ROLLUP` and `GROUP BY GROUPING SETS` does not to be supported either.
```sql
SELECT
    warehouse_country,
    warehouse_name,
    sensor_type,
    MEASURE(total_active_installed_sections) as total_active_installed_sections,
    MEASURE(total_sections) as total_sections,
    MEASURE(total_active_installed_sections) /  MEASURE(total_sections) as active_installations_coverage
FROM installations_metrics
GROUP BY ROLLUP (warehouse_country, warehouse_name, sensor_type)
ORDER BY warehouse_country, warehouse_name
```
and 
```sql
SELECT
    warehouse_country,
    warehouse_name,
    MEASURE(total_active_installed_sections) as total_active_installed_sections,
    MEASURE(total_sections) as total_sections,
    MEASURE(total_active_installed_sections) /  MEASURE(total_sections) as active_installations_coverage
FROM installations_metrics
GROUP BY GROUPING SETS ((warehouse_country), (warehouse_country, warehouse_name))
ORDER BY warehouse_country, warehouse_name
```

both would fail at Spark version `4.2.0`.

#### References
More on metrics view and semantic layer can be found here:
References
- https://books.japila.pl/spark-sql-internals/metric-views/
- https://books.japila.pl/spark-sql-internals/sql/SparkSqlAstBuilder/#visitCacheTable
- https://medium.com/@cheegodchang/why-do-we-need-unity-catalog-metric-views-455ce259519e
- https://docs.databricks.com/aws/en/uc-semantics/metric-views/yaml-reference
- https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-view
- https://issues.apache.org/jira/browse/SPARK-60399 - Jira for Spark native documentation
- https://github.com/apache/spark/pull/59386 - PR with Spark native documentation
- https://docs.google.com/document/d/1xVTLijvDTJ90lZ_ujwzf9HvBJgWg0mY6cYM44Fcghl0/edit?tab=t.0#heading=h.5fdy0uyv19hp - "SPIP: Metrics & semantic modeling in Spark" google doc
