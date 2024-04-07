# Databricks notebook source
def mount_adls(storage_account_name, container_name):
    # Get secrets from Key Vault
    client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-clientid')
    tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-tenantid')
    client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-clientsecret')

    # Set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": client_id,
              "fs.azure.account.oauth2.client.secret": client_secret,
              "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    # Mount the storage account container
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)

# COMMAND ----------

mount_adls('formula1dlsree', 'raw')

# COMMAND ----------

mount_adls('formula1dlsree', 'transforrmed')

# COMMAND ----------

mount_adls('formula1dlsree', 'presented')
