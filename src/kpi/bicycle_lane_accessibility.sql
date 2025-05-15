-- Accessibility KPI by census tract and trimester
WITH census_centroids AS (
    -- Get geometric centroid of each census tract
    SELECT
        census_tract_id,
        ST_Centroid(geometry) AS centroid
    FROM
        dim_location
),
-- Find the nearest bike lane per census tract and trimester
-- using spatial index and limiting the search
nearest_lane_distances AS (
    SELECT
        cc.census_tract_id,
        t.year_trimester,
        (
            SELECT MIN(ST_Distance(cc.centroid::geography, bs.geometry::geography))
            FROM fact_bicycle_lane_state bs
            WHERE bs.year_trimester = t.year_trimester
            -- Add a bounding box filter to reduce computation
            AND ST_DWithin(cc.centroid, bs.geometry, 0.1) -- ~11km in degrees
            LIMIT 100 -- Limit the number of lanes to check
        ) AS min_distance_meters
    FROM
        census_centroids cc
    CROSS JOIN
        dim_trimester t
)
SELECT
    nld.census_tract_id,
    l.neighbourhood_name,
    l.district_name,
    nld.year_trimester,
    t.year,
    t.trimester,
    nld.min_distance_meters,
    -- Normalized accessibility score (inverse of distance, higher is better)
    CASE
        WHEN nld.min_distance_meters = 0 THEN 1.0
        WHEN nld.min_distance_meters IS NULL THEN 0.0 -- No lanes in this trimester
        ELSE 1.0 / (1.0 + (nld.min_distance_meters / 1000))
    END AS accessibility_score
FROM
    nearest_lane_distances nld
JOIN
    dim_location l ON nld.census_tract_id = l.census_tract_id
JOIN
    dim_trimester t ON nld.year_trimester = t.year_trimester
ORDER BY
    t.year_trimester, accessibility_score DESC
LIMIT 1000;