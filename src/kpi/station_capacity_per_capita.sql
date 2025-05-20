WITH latest_station_capacity AS (
    -- Get the most recent capacity for each station
    SELECT DISTINCT ON (station_id)
        station_id,
        capacity
    FROM 
        fact_station_information
    ORDER BY 
        station_id, last_updated DESC
),
station_capacity_by_tract AS (
    -- Calculate total station capacity per census tract
    SELECT 
        l.census_tract_id,
        l.district_name,
        SUM(lsc.capacity) as total_capacity
    FROM 
        latest_station_capacity lsc
    JOIN 
        dim_station s ON lsc.station_id = s.station_id
    JOIN 
        dim_location l ON ST_Contains(l.geometry, s.geometry)
    GROUP BY 
        l.census_tract_id, l.district_name
),
district_area AS (
    -- Calculate total area per district
    SELECT 
        district_name,
        SUM(census_tract_area) as district_area
    FROM 
        dim_location
    GROUP BY 
        district_name
),
district_metrics AS (
    -- Aggregate metrics by district
    SELECT 
        sc.district_name,
        pi.year,
        SUM(pi.population) as total_population,
        AVG(pi.income_euros) as avg_income,
        SUM(sc.total_capacity) as total_capacity,
        da.district_area,
        -- Population density (per sq km)
        (SUM(pi.population) / NULLIF(da.district_area, 0)) as population_density,
        -- Capacity per 1000 inhabitants per sq km
        ROUND(((SUM(sc.total_capacity)::numeric / NULLIF(SUM(pi.population), 0)) * 1000 / NULLIF(da.district_area, 0))::numeric, 4) as capacity_per_1000_inhabitants_per_sqkm
    FROM 
        station_capacity_by_tract sc
    JOIN 
        fact_population_income pi ON sc.census_tract_id = pi.census_tract_id
    JOIN 
        district_area da ON sc.district_name = da.district_name
    GROUP BY 
        sc.district_name, pi.year, da.district_area
),
latest_year_metrics AS (
    -- Get metrics for the latest year only
    SELECT *
    FROM district_metrics
    WHERE year = (SELECT MAX(year) FROM district_metrics)
),
city_averages AS (
    -- Calculate citywide averages for fair distribution reference points
    SELECT
        AVG(capacity_per_1000_inhabitants_per_sqkm) as avg_city_capacity_density,
        AVG(avg_income) as avg_city_income
    FROM
        latest_year_metrics
)
SELECT 
    lym.district_name,
    lym.year,
    lym.total_population,
    lym.avg_income,
    lym.capacity_per_1000_inhabitants_per_sqkm,
    
    -- Flag districts with potential discrimination (high need, low service)
    CASE 
        -- Fair Distribution: within +-10% of city averages for both income and capacity
        WHEN lym.avg_income BETWEEN ca.avg_city_income * 0.9 AND ca.avg_city_income * 1.1 
             AND lym.capacity_per_1000_inhabitants_per_sqkm BETWEEN ca.avg_city_capacity_density * 0.9 AND ca.avg_city_capacity_density * 1.1
        THEN 'Fair Distribution'
        -- Standard quadrants
        WHEN lym.avg_income < ca.avg_city_income * 0.9 AND lym.capacity_per_1000_inhabitants_per_sqkm < ca.avg_city_capacity_density * 0.9
        THEN 'Potential Underservice'
        WHEN lym.avg_income > ca.avg_city_income * 1.1 AND lym.capacity_per_1000_inhabitants_per_sqkm > ca.avg_city_capacity_density * 1.1
        THEN 'Privileged Access'
        WHEN lym.avg_income < ca.avg_city_income * 0.9 AND lym.capacity_per_1000_inhabitants_per_sqkm > ca.avg_city_capacity_density * 1.1
        THEN 'Good Service Despite Low Income'
        WHEN lym.avg_income > ca.avg_city_income * 1.1 AND lym.capacity_per_1000_inhabitants_per_sqkm < ca.avg_city_capacity_density * 0.9
        THEN 'Poor Service Despite High Income'
        -- For values that fall in between (outside fair zone but not extreme enough for other categories)
        ELSE 'Moderate Distribution'
    END as equity_status
FROM 
    latest_year_metrics lym
CROSS JOIN
    city_averages ca
ORDER BY 
    district_name; 