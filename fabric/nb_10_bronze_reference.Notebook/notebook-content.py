# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
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
import pandas as pd
from deltalake import write_deltalake

token = notebookutils.credentials.getToken("storage")
options = {"bearer_token": token, "use_fabric_endpoint": "true"}

folder = "/lakehouse/default/Files/landing/reference"
base = "abfss://SOLAR-dev@onelake.dfs.fabric.microsoft.com/lh_solar.Lakehouse/Tables/bronze"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

for name in sorted(os.listdir(folder)):
    df = pd.read_csv(f"{folder}/{name}", dtype=str, keep_default_na=False)
    df["source_file"] = name
    table = name.replace(".csv", "")
    write_deltalake(f"{base}/{table}", df, mode="overwrite", storage_options=options)
    print(table, df.shape)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
