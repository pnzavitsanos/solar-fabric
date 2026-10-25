# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "eebf2362-c472-4305-a295-fa1687f08ce8",
# META       "default_lakehouse_name": "lh_solar",
# META       "default_lakehouse_workspace_id": "8c6a8652-2496-4d07-9384-5408f4d9dbce",
# META       "known_lakehouses": [
# META         {
# META           "id": "eebf2362-c472-4305-a295-fa1687f08ce8"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SET spark.sql.session.timeZone = UTC


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""

SELECT
  lower(trim(device_id))                               AS device_hash_id,
  timestamp_millis(CAST(collect_time AS BIGINT))       AS ts,
  try_cast(replace(active_power, ',', '.') AS DOUBLE)  AS ac_power_kw,
  try_cast(replace(dc_power, ',', '.') AS DOUBLE)      AS dc_power_kw,
  try_cast(replace(efficiency, ',', '.') AS DOUBLE)    AS efficiency_pct,
  try_cast(replace(day_cap, ',', '.') AS DOUBLE)       AS energy_today_kwh,
  try_cast(replace(total_cap, ',', '.') AS DOUBLE)     AS energy_total_kwh,
  try_cast(replace(temperature, ',', '.') AS DOUBLE)   AS temperature_c,
  inverter_state                                       AS state_code,
  source_file,
  'fusionsolar'                                        AS vendor
FROM bronze.readings_fusionsolar
""").createOrReplaceTempView("fs_clean")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""

SELECT
  lower(trim(inverter_id))                              AS device_hash_id,
  coalesce(try_to_timestamp(`timestamp`, 'yyyy-MM-dd HH:mm:ss'),
           try_to_timestamp(`timestamp`, 'dd/MM/yyyy HH:mm:ss')) - INTERVAL 1 HOUR AS ts,
  try_cast(replace(total_ac_power, ',', '.') AS DOUBLE) AS ac_power_kw,
  try_cast(replace(dc_power, ',', '.') AS DOUBLE)       AS dc_power_kw,
  CAST(NULL AS DOUBLE)                                  AS efficiency_pct,
  try_cast(replace(eToday, ',', '.') AS DOUBLE)         AS energy_today_kwh,
  try_cast(replace(eTotal, ',', '.') AS DOUBLE)         AS energy_total_kwh,
  try_cast(replace(temperature, ',', '.') AS DOUBLE)    AS temperature_c,
  state                                                 AS state_code,
  source_file,
  'solis'                                               AS vendor
FROM bronze.readings_solis
""").createOrReplaceTempView("so_clean")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW readings_clean AS
# MAGIC SELECT * FROM fs_clean
# MAGIC UNION ALL
# MAGIC SELECT * FROM so_clean

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT vendor, COUNT(*) AS rows, COUNT(ts) AS with_time, MIN(ts) AS first, MAX(ts) AS last
# MAGIC FROM readings_clean
# MAGIC GROUP BY vendor

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW readings_dedup AS
# MAGIC SELECT * FROM (
# MAGIC   SELECT *,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY vendor, device_hash_id, ts
# MAGIC                             ORDER BY source_file DESC) AS rn
# MAGIC   FROM readings_clean
# MAGIC )
# MAGIC WHERE rn = 1

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT vendor, COUNT(*) AS rows
# MAGIC FROM readings_dedup
# MAGIC GROUP BY vendor

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
SELECT 'fusionsolar' AS vendor, state_id, canonical_state_id FROM bronze.inverter_state_mapping_fusionsolar
UNION ALL
SELECT 'solis' AS vendor, state_id, canonical_state_id FROM bronze.inverter_state_mapping_solis
""").createOrReplaceTempView("states")

spark.sql("""
SELECT device_hash_id, try_cast(max_power AS DOUBLE) AS max_power_kw
FROM bronze.devices
""").createOrReplaceTempView("device_size")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW readings_flagged AS
# MAGIC SELECT
# MAGIC   r.device_hash_id, r.ts, r.ac_power_kw, r.dc_power_kw, r.efficiency_pct,
# MAGIC   r.energy_today_kwh, r.energy_total_kwh, r.temperature_c, r.state_code,
# MAGIC   r.source_file, r.vendor,
# MAGIC   d.device_hash_id IS NULL                                   AS unknown_device,
# MAGIC   coalesce(r.temperature_c > 100, false)                     AS bad_temperature,
# MAGIC   coalesce(r.energy_total_kwh = 0, false)                    AS bad_counter,
# MAGIC   s.state_id IS NULL                                         AS unknown_state,
# MAGIC   coalesce(r.ac_power_kw > d.max_power_kw * 2, false)        AS too_much_power,
# MAGIC   coalesce(r.energy_today_kwh > d.max_power_kw * 24, false)  AS bad_energy_today,
# MAGIC   s.canonical_state_id,
# MAGIC   CASE s.canonical_state_id WHEN '1' THEN 'ON' WHEN '2' THEN 'OFF' WHEN '3' THEN 'ERROR' END AS state
# MAGIC FROM readings_dedup r
# MAGIC LEFT JOIN device_size d ON r.device_hash_id = d.device_hash_id
# MAGIC LEFT JOIN states s      ON r.vendor = s.vendor AND r.state_code = s.state_id

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT vendor, COUNT(*) AS rows,
# MAGIC   SUM(CAST(unknown_device AS INT))   AS unknown_device,
# MAGIC   SUM(CAST(bad_temperature AS INT))  AS bad_temperature,
# MAGIC   SUM(CAST(bad_counter AS INT))      AS bad_counter,
# MAGIC   SUM(CAST(unknown_state AS INT))    AS unknown_state,
# MAGIC   SUM(CAST(too_much_power AS INT))   AS too_much_power,
# MAGIC   SUM(CAST(bad_energy_today AS INT)) AS bad_energy_today
# MAGIC FROM readings_flagged
# MAGIC GROUP BY vendor

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("readings_flagged").write.mode("overwrite").saveAsTable("silver.readings")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT vendor, state, COUNT(*) AS n
# MAGIC FROM silver.readings
# MAGIC GROUP BY vendor, state
# MAGIC ORDER BY vendor, state

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
