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

import os
from pyspark.sql.functions import col

folder = "/lakehouse/default/Files/landing/solis"
comma_files = []
semi_files = []

for name in sorted(os.listdir(folder)):
    with open(f"{folder}/{name}") as f:
        header = f.readline()
    path = f"Files/landing/solis/{name}"
    if ";" in header:
        semi_files.append(path)
    else:
        comma_files.append(path)

print(len(comma_files), len(semi_files))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

so_comma = spark.read.csv(comma_files, header=True, sep=",").withColumn("source_file", col("_metadata.file_name"))
so_semi = spark.read.csv(semi_files, header=True, sep=";").withColumn("source_file", col("_metadata.file_name"))

so = so_comma.unionByName(so_semi)
print(so.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

so.write.mode("overwrite").saveAsTable("bronze.readings_solis")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
