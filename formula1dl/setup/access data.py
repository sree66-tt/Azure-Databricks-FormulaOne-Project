# Databricks notebook source
client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-clientid')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-tenantid')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-clientsecret')


# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlsree.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dlsree.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dlsree.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dlsree.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dlsree.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@formula1dlsree.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://raw@formula1dlsree.dfs.core.windows.net/circuits.csv"))


# COMMAND ----------

dbutils.secrets.list(scope = 'formula1-scope')

# COMMAND ----------

dbutils.secrets.listScopes()
