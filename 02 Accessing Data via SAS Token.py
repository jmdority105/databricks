# Databricks notebook source
# MAGIC %md
# MAGIC # Accessing Data via SAS Token
# MAGIC 
# MAGIC #### Resources:
# MAGIC * https://learn.microsoft.com/en-us/azure/databricks/external-data/azure-storage#access-azure-data-lake-storage-gen2-or-blob-storage-using-a-sas-token
# MAGIC * https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri

# COMMAND ----------

# Setting the configuration - remember to remove ? from token if present - 1st character
spark.conf.set("fs.azure.account.auth.type.datalakejmdtest.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.datalakejmdtest.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.datalakejmdtest.dfs.core.windows.net", "sv=2021-12-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-04-15T02:59:44Z&st=2023-04-03T18:59:44Z&spr=https&sig=P2hOCG5pUCcUVDrp3TUsz96K1dEEdDyU8LUSPCQo4xE%3D")

# COMMAND ----------

# Reading data from storage account
spark.read.csv("abfss://bronze@datalakejmdtest.dfs.core.windows.net/country_regions.csv", header=True).display()
