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
