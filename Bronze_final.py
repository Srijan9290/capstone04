# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt


# COMMAND ----------

    def rename_columns(df):
        for col_name in df.columns:
            new_col_name = col_name.replace(" ", "_")
            df = df.withColumnRenamed(col_name, new_col_name)
        return df

# COMMAND ----------

@dlt.create_table(
  comment="The Accounts table",
  table_properties={
    "Wetrust.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def accounts_raw():
    """
    Load and create the Raw accounts Delta Lake table.

    This function reads data from a CSV file, processes it, and stores it in a Delta Lake table.
    The table represents the raw, unprocessed data of accounts.

    Returns:
        pyspark.sql.DataFrame: A Spark SQL DataFrame containing the raw customer data.
    """
    accounts_df = spark.read.csv("dbfs:/mnt/capstone_bronze/dataset/unzipped/accounts.csv", header=True, inferSchema=True)
    return accounts_df

# COMMAND ----------

@dlt.create_table(
  comment="The branches table",
  table_properties={
    "Wetrust.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def branches_raw():
    """
    Load and create the Raw branches Delta Lake table.

    This function reads data from a CSV file, processes it, and stores it in a Delta Lake table.
    The table represents the raw, unprocessed data of branches.

    Returns:
        pyspark.sql.DataFrame: A Spark SQL DataFrame containing the raw customer data.
    """
    branches_df = spark.read.csv("dbfs:/mnt/capstone_bronze/dataset/unzipped/branches.csv", header=True, inferSchema=True)
    return branches_df

# COMMAND ----------

from delta import DeltaTable

@dlt.create_table(
  comment="The credits table",
  table_properties={
    "Wetrust.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def credits_raw():
    """
    Load and create the Raw credits Delta Lake table.

    This function reads data from a json file, processes it, and stores it in a Delta Lake table.
    The table represents the raw, unprocessed data of credits.

    Args:
        spark: SparkSession object.

    Returns:
        pyspark.sql.DataFrame: A Spark SQL DataFrame containing the raw customer data.
    """
    # Read JSON data into a DataFrame
    credits_df = spark.read.option("multiline", "true").json("dbfs:/mnt/capstone_bronze/dataset/unzipped/credit.json")

    credits_df = rename_columns(credits_df)
    return credits_df


# COMMAND ----------

@dlt.create_table(
  comment="The customers table",
  table_properties={
    "Wetrust.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def customers_raw():
    """
    Load and create the Raw customers Delta Lake table.

    This function reads data from a CSV file, processes it, and stores it in a Delta Lake table.
    The table represents the raw, unprocessed data of customers.

    Returns:
        pyspark.sql.DataFrame: A Spark SQL DataFrame containing the raw customer data.
    """
    customers_df = spark.read.csv("dbfs:/mnt/capstone_bronze/dataset/unzipped/customers.csv", header=True, inferSchema=True)
    return customers_df

# COMMAND ----------

@dlt.create_table(
  comment="The transactions table",
  table_properties={
    "Wetrust.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def transactions_raw():
    """
    Load and create the Raw transactions Delta Lake table.

    This function reads data from a CSV file, processes it, and stores it in a Delta Lake table.
    The table represents the raw, unprocessed data of transactions.

    Returns:
        pyspark.sql.DataFrame: A Spark SQL DataFrame containing the raw customer data.
    """
    transactions_df = spark.read.csv("dbfs:/mnt/capstone_bronze/dataset/unzipped/transactions.csv", header=True, inferSchema=True)
    return transactions_df

# COMMAND ----------

@dlt.create_table(
  comment="The loans table",
  table_properties={
    "Wetrust.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def loans_raw():
    """
    Load and create the Raw loans Delta Lake table.

    This function reads data from a CSV file, processes it, and stores it in a Delta Lake table.
    The table represents the raw, unprocessed data of loans.

    Returns:
        pyspark.sql.DataFrame: A Spark SQL DataFrame containing the raw customer data.
    """
    loans_df = spark.read.csv("dbfs:/mnt/capstone_bronze/dataset/unzipped/loans_partition_1.csv", header=True, inferSchema=True)
    loans_df = rename_columns(loans_df)
    return loans_df

# COMMAND ----------


