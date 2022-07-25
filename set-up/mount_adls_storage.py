# Databricks notebook source
storage_account_name = "formula1newdl"
client_id = "20edf765-f348-4138-a8a0-0cecd4ca58e7"
tenant_id = "d15f8f49-b1d8-4b09-80a8-5e7bb0a13bba"
client_secret = "rv58Q~CeAUBR1HcmUOKlNJxjD1O2VOE1wij~Jbq3"
container_name = "raw"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

#mount raw continer
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)

# COMMAND ----------

#check if mounter correctly -method1
dbutils.fs.ls("/mnt/formula1newdl/raw")

# COMMAND ----------

#check if mounter correctly -method2
dbutils.fs.mounts()

# COMMAND ----------

def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

#mount processed container
mount_adls("processed")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1newdl/processed")

# COMMAND ----------


