# Databricks notebook source
# MAGIC %md
# MAGIC # Mounting ADLS to DBFS
# MAGIC 
# MAGIC #### Resources:
# MAGIC * https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts

# COMMAND ----------

#tenant (directory) id d00e44c8-500b-4d40-a583-e0aaa5613528
#application (client) id 867563a5-2771-477c-9a23-1b8bc63e461c
#client secret qEw8Q~hNv_q-2hRHucW-dGqH6x0F1SLAYBIqbaKp
#secret id c96d8d1a-bd4a-4dc2-b0f1-149f78b7dc77

# COMMAND ----------

# syntax for configs and mount methods
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "867563a5-2771-477c-9a23-1b8bc63e461c",
          "fs.azure.account.oauth2.client.secret":  "qEw8Q~hNv_q-2hRHucW-dGqH6x0F1SLAYBIqbaKp",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/d00e44c8-500b-4d40-a583-e0aaa5613528/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://bronze@datalakejmdtest.dfs.core.windows.net/",
  mount_point = "/mnt/bronze",
  extra_configs = configs)

# COMMAND ----------

# Using dbutils to display mounts
display(dbutils.fs.mounts())

# COMMAND ----------

# Using magic commands - this one doesn't work 
%fs
ls

# COMMAND ----------

# Using dbutils to display directories and files
display(dbutils.fs.ls('/'))

# COMMAND ----------

application_id = "867563a5-2771-477c-9a23-1b8bc63e461c"

tenant_id = "d00e44c8-500b-4d40-a583-e0aaa5613528"

secret = "a8c06164-8f3b-4005-852a-27812a37c077"

# COMMAND ----------

# Performing the mount
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "a8c06164-8f3b-4005-852a-27812a37c077",
          "fs.azure.account.oauth2.client.secret": "a8c06164-8f3b-4005-852a-27812a37c077",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/a43c85f4-0351-4195-800b-cea7508b758c/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://bronze@ddatalakejmdtest.dfs.core.windows.net/",
  mount_point = "/mnt/bronze",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# Reading data from the new mount point
spark.read.csv("/mnt/bronze/countries.csv", header=True).display()

# COMMAND ----------

# Using the unmount method to unmount the storage container
dbutils.fs.unmount('/mnt/bronze')
