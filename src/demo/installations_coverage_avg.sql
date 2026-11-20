SELECT
    warehouse_name,
    MEASURE(total_active_installed_sections) as total_active_installed_sections,
    MEASURE(total_sections) as total_sections,
    MEASURE(total_active_installed_sections) / MEASURE(total_sections) as installations_coverage
FROM installations_metrics
GROUP BY warehouse_name
