# Databricks notebook source
# MAGIC %md
# MAGIC # Secret Scopes
# MAGIC 
# MAGIC #### Resources:
# MAGIC * Secret Scopes: https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes
# MAGIC * Secret Management: https://learn.microsoft.com/en-us/azure/databricks/security/secrets/

# COMMAND ----------

tenant_id = "d00e44c8-500b-4d40-a583-e0aaa5613528"
application_id = "867563a5-2771-477c-9a23-1b8bc63e461c"
secret ="qEw8Q~hNv_q-2hRHucW-dGqH6x0F1SLAYBIqbaKp"
#secret id c96d8d1a-bd4a-4dc2-b0f1-149f78b7dc77

# COMMAND ----------

# Once you create a key vault, register your secrets and create a secret scope. You can access them via dbutils  
dbutils.secrets.get(scope="YOUR SCOPE HERE", key="YOUR SECRET KEY HERE")

# COMMAND ----------

application_id = dbutils.secrets.get(scope="databricks-keyvault-123", key="application_id")
tenant_id = dbutils.secrets.get(scope="databricks-keyvault-123", key="tenant_id")
secret = dbutils.secrets.get(scope="databricks-keyvault-123", key="secret")

# COMMAND ----------

container_name = 'bronze'
account_name = 'datalakejmdtest'
mount_point = '/mnt/bronze'

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs)
