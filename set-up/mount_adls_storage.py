# Databricks notebook source
storage_account_name = "formula1newdl"
#help not accidently expose secrets, but not actually block someone to see the keys
client_id            = dbutils.secrets.get(scope="formula1-scope", key="databricks-app-client-id")
tenant_id            = dbutils.secrets.get(scope="formula1-scope", key="databricks-app-tenant-id")
client_secret        = dbutils.secrets.get(scope="formula1-scope", key="databricks-client-secret-value")

#storage_account_name = "formula1newdl"
#client_id = "20edf765-f348-4138-a8a0-0cecd4ca58e7"
#tenant_id = "d15f8f49-b1d8-4b09-80a8-5e7bb0a13bba"
#client_secret = "rv58Q~CeAUBR1HcmUOKlNJxjD1O2VOE1wij~Jbq3"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

#mount container
def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

#mount raw container
mount_adls("raw")

# COMMAND ----------

#mount processed container
mount_adls("processed")

# COMMAND ----------

#listing what's in
dbutils.fs.ls("/mnt/formula1newdl/raw")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1newdl/processed")
