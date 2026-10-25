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
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SHOW SCHEMAS

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
