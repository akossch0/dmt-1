-- Connectivity KPI using pre-calculated fact table
SELECT
    n.year_trimester,
    t.year,
    t.trimester,
    n.total_lanes,
    n.connected_lanes,
    n.isolated_lanes,
    n.connectivity_ratio AS connectivity_score
FROM
    fact_bike_network_metrics n
JOIN
    dim_trimester t ON n.year_trimester = t.year_trimester
ORDER BY
    t.year, t.trimester;
