# Databricks notebook source
container_name ="container1"
account_name = "capstone004"
storage_account_key="8I0hjFw2QdswQPvM2soP8A40nkErcuq/ThGbgYOf/bvjdg/3odiC9l6Y58FIAyYTOSbV3RC2BS2S+AStro7E3g=="
mount_point = "/mnt/capstone_bronze"
# Check if the storage is already mounted
mounts = dbutils.fs.mounts()
is_mounted = False
for mount in mounts:
  if mount.mountPoint == mount_point:
    is_mounted = True

# If not mounted, then mount it
if not is_mounted:
    dbutils.fs.mount(
      source='wasbs://{0}@{1}.blob.core.windows.net'.format(container_name,account_name),
      mount_point = mount_point,
      extra_configs  = {"fs.azure.account.key.capstone004.blob.core.windows.net" : storage_account_key}
    )
    print(f"Storage mounted at {mount_point}")
else:
    print(f"Storage is already mounted at {mount_point}")

# COMMAND ----------

customers_df = spark.read.format("csv").option("inferSchema","true").option("header","true").load("/mnt/capstone_bronze/customers.csv")

# COMMAND ----------

accounts_df = spark.read.format("csv").option("inferSchema","true").option("header","true").load("/mnt/capstone_bronze/accounts.csv")

# COMMAND ----------

branches_df = spark.read.format("csv").option("inferSchema","true").option("header","true").load("/mnt/capstone_bronze/branches.csv")

# COMMAND ----------

loans_partition_df = spark.read.format("csv").option("inferSchema","true").option("header","true").load("/mnt/capstone_bronze/loans_partition_1.csv")

# COMMAND ----------

transactions_df = spark.read.format("csv").option("inferSchema","true").option("header","true").load("/mnt/capstone_bronze/transactions.csv")

# COMMAND ----------

credit_df = spark.read.format("json").option("inferSchema","true").option("header","true").option("multiliine","true").load("/mnt/capstone_bronze/credit.json")

# COMMAND ----------

customers_df

# COMMAND ----------

transactions_df

# COMMAND ----------

loans_partition_df

# COMMAND ----------

accounts_df

# COMMAND ----------

branches_df

# COMMAND ----------

credit_df

# COMMAND ----------


