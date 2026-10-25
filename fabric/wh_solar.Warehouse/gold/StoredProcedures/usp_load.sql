CREATE   PROCEDURE gold.usp_load
AS
BEGIN
    DROP TABLE IF EXISTS gold.dim_state;
    CREATE TABLE gold.dim_state AS
    SELECT canonical_state_id, description,
           CASE canonical_state_id WHEN '1' THEN 'ON' WHEN '2' THEN 'OFF' WHEN '3' THEN 'ERROR' END AS state
    FROM lh_solar.bronze.inverter_state_canonical;

    DROP TABLE IF EXISTS gold.dim_station;
    CREATE TABLE gold.dim_station AS
    SELECT st.station_hash_id, st.name,
           TRY_CAST(NULLIF(st.location_latitude, '') AS float) AS latitude,
           TRY_CAST(NULLIF(st.plant_age_years, '') AS float)   AS plant_age_years,
           LOWER(src.name)                                     AS vendor
    FROM lh_solar.bronze.stations st
    JOIN lh_solar.bronze.source src ON st.source = src.id;

    DROP TABLE IF EXISTS gold.dim_device;
    CREATE TABLE gold.dim_device AS
    SELECT d.device_hash_id, d.station_hash_id, d.name, d.device_model,
           TRY_CAST(NULLIF(d.max_power, '') AS float) AS max_power_kw,
           LOWER(src.name)                            AS vendor
    FROM lh_solar.bronze.devices d
    JOIN lh_solar.bronze.source src ON d.source = src.id;

    DROP TABLE IF EXISTS gold.dim_date;
    CREATE TABLE gold.dim_date AS
    SELECT
      d.[date],
      YEAR(d.[date]) * 10000 + MONTH(d.[date]) * 100 + DAY(d.[date])  AS date_key,
      YEAR(d.[date])                                                  AS [year],
      MONTH(d.[date])                                                 AS [month],
      CAST(DATENAME(month, d.[date]) AS varchar(10))                  AS month_name,
      CAST(DATENAME(weekday, d.[date]) AS varchar(10))                AS weekday,
      CAST(CASE WHEN DATENAME(weekday, d.[date]) IN ('Saturday', 'Sunday')
                THEN 1 ELSE 0 END AS bit)                             AS is_weekend
    FROM (
      SELECT DATEADD(day, a.n + 10 * b.n + 100 * c.n, r.first_day) AS [date], r.last_day
      FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS a(n)
      CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS b(n)
      CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS c(n)
      CROSS JOIN (SELECT MIN(CAST(ts AS date)) AS first_day,
                         MAX(CAST(ts AS date)) AS last_day
                  FROM lh_solar.silver.readings) AS r
    ) AS d
    WHERE d.[date] <= d.last_day;

    DROP TABLE IF EXISTS gold.fact_readings;
    CREATE TABLE gold.fact_readings AS
    SELECT
      YEAR(r.ts) * 10000 + MONTH(r.ts) * 100 + DAY(r.ts)  AS date_key,
      r.device_hash_id, d.station_hash_id, r.canonical_state_id, r.ts,
      r.ac_power_kw, r.dc_power_kw, r.efficiency_pct,
      r.energy_today_kwh, r.energy_total_kwh, r.temperature_c,
      CAST(CASE WHEN r.unknown_device = 0 AND r.bad_temperature = 0 AND r.bad_counter = 0
                 AND r.unknown_state = 0 AND r.too_much_power = 0 AND r.bad_energy_today = 0
                THEN 1 ELSE 0 END AS bit)                 AS is_valid,
      r.source_file
    FROM lh_solar.silver.readings AS r
    LEFT JOIN gold.dim_device AS d ON r.device_hash_id = d.device_hash_id;

    DROP TABLE IF EXISTS gold.fact_daily_energy;
    CREATE TABLE gold.fact_daily_energy AS
    SELECT date_key, station_hash_id, device_hash_id,
           MAX(energy_today_kwh) AS energy_kwh
    FROM gold.fact_readings
    WHERE is_valid = 1
    GROUP BY date_key, station_hash_id, device_hash_id;
END;

GO