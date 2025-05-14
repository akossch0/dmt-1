-- Coverage KPI using pre-calculated fact table
SELECT
    l.census_tract_id,
    l.neighbourhood_name,
    l.district_name,
    m.year_trimester,
    t.year,
    t.trimester,
    m.total_lane_length,
    m.coverage_score
FROM
    fact_bike_tract_metrics m
JOIN
    dim_location l ON m.census_tract_id = l.census_tract_id
JOIN
    dim_trimester t ON m.year_trimester = t.year_trimester
ORDER BY
    t.year_trimester, m.coverage_score DESC;