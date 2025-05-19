CREATE MATERIALIZED VIEW mv_bike_availability_by_district_10min AS
WITH station_district AS (
    SELECT
        s.station_id,
        l.district_name,
        l.district_code
    FROM
        dim_station s
    JOIN
        dim_location l ON ST_Contains(l.geometry, s.geometry)
),
station_metrics AS (
    SELECT
        sd.district_name,
        sd.district_code,
        h.date_value,
        h.hour,
        t.ten_min_datetime,
        ss.station_id,
        ss.num_bikes_available,
        ss.mechanical_bikes,
        ss.ebikes,
        ss.num_docks_available,
        CASE
            WHEN (ss.num_bikes_available + ss.num_docks_available) > 0 THEN
                (ss.num_bikes_available::NUMERIC / (ss.num_bikes_available + ss.num_docks_available)::NUMERIC) * 100
            ELSE 0
        END AS bike_availability_percentage,
        CASE
            WHEN ss.num_bikes_available > 0 THEN
                (ss.ebikes::NUMERIC / ss.num_bikes_available::NUMERIC) * 100
            ELSE 0
        END AS ebike_percentage,
        CASE
            WHEN ss.num_bikes_available > 0 THEN
                (ss.mechanical_bikes::NUMERIC / ss.num_bikes_available::NUMERIC) * 100
            ELSE 0
        END AS mechanical_bike_percentage
    FROM
        fact_station_status ss
    JOIN
        dim_ten_minute t ON ss.ten_min_datetime = t.ten_min_datetime
    JOIN
        dim_hour h ON t.hour_datetime = h.hour_datetime
    JOIN
        dim_day d ON h.date_value = d.date_value
    JOIN
        dim_month m ON d.year_month = m.year_month
    JOIN
        station_district sd ON ss.station_id = sd.station_id
    WHERE
        m.year = 2019 AND
        m.month = 6 AND
        d.date_value BETWEEN '2019-06-01' AND '2019-06-07' -- One week of data
)
SELECT
    district_name,
    district_code,
    date_value,
    hour,
    ten_min_datetime,
    AVG(bike_availability_percentage) AS avg_bike_availability,
    AVG(ebike_percentage) AS avg_ebike_percentage,
    AVG(mechanical_bike_percentage) AS avg_mechanical_bike_percentage,
    SUM(num_bikes_available) AS total_bikes_available,
    SUM(ebikes) AS total_ebikes,
    SUM(mechanical_bikes) AS total_mechanical_bikes,
    COUNT(DISTINCT station_id) AS station_count
FROM
    station_metrics
GROUP BY
    district_name, district_code, date_value, hour, ten_min_datetime
ORDER BY
    ten_min_datetime, district_name;

-- Create indexes for performance
CREATE INDEX idx_mv_bike_10min_datetime
ON mv_bike_availability_by_district_10min(ten_min_datetime);

CREATE INDEX idx_mv_bike_10min_district
ON mv_bike_availability_by_district_10min(district_code);