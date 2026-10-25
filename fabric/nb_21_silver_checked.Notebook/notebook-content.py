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
# MAGIC CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS silver.readings_checked
# MAGIC (
# MAGIC   CONSTRAINT known_device   CHECK (unknown_device = false)   ON MISMATCH DROP,
# MAGIC   CONSTRAINT sane_temp      CHECK (bad_temperature = false)  ON MISMATCH DROP,
# MAGIC   CONSTRAINT counter_not_0  CHECK (bad_counter = false)      ON MISMATCH DROP,
# MAGIC   CONSTRAINT known_state    CHECK (unknown_state = false)    ON MISMATCH DROP,
# MAGIC   CONSTRAINT sane_power     CHECK (too_much_power = false)   ON MISMATCH DROP,
# MAGIC   CONSTRAINT sane_energy    CHECK (bad_energy_today = false) ON MISMATCH DROP
# MAGIC )
# MAGIC AS SELECT * FROM silver.readings


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) FROM silver.readings_checked

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
