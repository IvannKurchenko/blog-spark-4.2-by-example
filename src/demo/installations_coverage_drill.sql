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
