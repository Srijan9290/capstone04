# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run "/capstone/Bronze_final"

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned accounts",
  #partition_cols=["account_id"],
  table_properties={
    "WeTrust_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_or_drop("valid account_id", "account_id IS NOT NULL")
@dlt.expect_or_drop("valid customer_id", "customer_id IS NOT NULL")
def accounts_clean():
    """
    Cleans and prepares customer data.

    Reads raw customer data from 'accounts_raw', performs necessary cleaning steps, and returns the cleaned DataFrame.

    Returns:
        DataFrame: A DataFrame containing cleaned customer data.
    """
    accounts_df = dlt.read('accounts_raw')
    accounts_df = accounts_df.select([col(column).alias(column.lower()) for column in accounts_df.columns])
    accounts_df = accounts_df.select([col(column).alias(column.lower()) for column in accounts_df.columns]).na.replace("?", None)
    accounts_df = accounts_df.withColumnRenamed("customerid", "customer_id").withColumnRenamed("AccountId", "account_id")
    accounts_df = accounts_df.dropDuplicates(["account_id"])
    accounts_df = accounts_df.fillna("missing data")
    accounts_df = accounts_df.withColumn("last_kyc_updated", date_format(col("last_kyc_updated"), "dd/MM/yyyy"))
    accounts_df = accounts_df.withColumn("account_created", date_format(col("account_created"), "dd/MM/yyyy"))
    accounts_df = accounts_df.withColumn("last_kyc_updated", to_date(col("last_kyc_updated"), "dd/MM/yyyy"))
    accounts_df = accounts_df.withColumn("account_created", to_date(col("account_created"), "dd/MM/yyyy"))
    # accounts_df.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy("account_id").saveAsTable("accounts_clean")
    return accounts_df

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned branch, ingested from Bronze",
  partition_cols=["bank_city"],
  table_properties={
    "WeTrust_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_or_drop("valid branch_id", "branch_id IS NOT NULL")
def branch_clean():
    """
    Clean and Normalize Branch DataFrame

    This function takes an input DataFrame `branch_df` and renames its columns to lowercase.

    Parameters:
    - branch_df (DataFrame): The input DataFrame containing branch data.

    Returns:
    - DataFrame: A cleaned and normalized DataFrame with lowercase column names.
    """
    branches_df = dlt.read('branches_raw')
    branches_df = branches_df.select([col(column).alias(column.lower()) for column in branches_df.columns])
    branches_df=branches_df.fillna("missing data")
    branches_df = branches_df.withColumnRenamed("branchid", "branch_id")
    # branches_df.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy("bank_city").saveAsTable("branches_clean")
    return branches_df

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned customers, ingested from Bronze",
  #partition_cols=["customer_id"],
  table_properties={
    "WeTrust_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_or_drop("valid customer_id", "customer_id IS NOT NULL")
@dlt.expect_all({"valid_phone":"len(Customer_Phone) == 10"})
def customers_clean():
    """
    Clean and Normalize Customers DataFrame

    This function takes an input DataFrame `customers_df`, removes duplicate rows based on the "customer_id" column, renames its columns to lowercase, and retains only the first occurrence of each customer based on "customer_id".

    Parameters:
    - customers_df (DataFrame): The input DataFrame containing customer data.

    Returns:
    - DataFrame: A cleaned and normalized DataFrame with lowercase column names and duplicates removed, keeping only the first occurrence of each customer.
"""
    customers_df = dlt.read('customers_raw')
    window = Window.partitionBy("customer_id").orderBy("customer_id")
    customers_df = customers_df.dropDuplicates(["customer_id"])
    customers_cleaned_df = customers_df.select([col(column).alias(column.lower()) for column in customers_df.columns]).withColumn("row",row_number().over(window)).filter(col("row") == 1).drop("row")
    customers_df = customers_cleaned_df.fillna("missing data")
    customers_df = customers_df.withColumn("dob", date_format(col("dob"), "dd/MM/yyyy"))
    customers_df = customers_df.withColumn("dob", to_date(col("dob"), "dd/MM/yyyy"))
    # customers_df.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy("customer_id").saveAsTable("customers_clean")
    return customers_df

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned transactions, ingested from Bronze",
  partition_cols=["transaction_id"],
  table_properties={
    "WeTrust_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_or_drop("valid transaction_id", "transaction_id IS NOT NULL")
@dlt.expect_or_drop("valid account_id", "account_id IS NOT NULL")
def transactions_clean():
    """
    Clean and Normalize Transactions DataFrame

    This function takes an input DataFrame `transactions_df`, renames its columns to lowercase, and renames the "accountid" column to "account_id" for consistency.

    Parameters:
    - transactions_df (DataFrame): The input DataFrame containing transaction data.

    Returns:
    - DataFrame: A cleaned and normalized DataFrame with lowercase column names and the "accountid" column renamed to "account_id".
"""
    transactions_df = dlt.read('transactions_raw')
    transactions_df = transactions_df.select([col(column).alias(column.lower()) for column in transactions_df.columns]).withColumnRenamed("accountid", "account_id").withColumn("year", year("transaction_date")).withColumn("month", month("transaction_date"))
    transactions_df= transactions_df.fillna("missing data")
    transactions_df = transactions_df.withColumn("transaction_date", date_format(col("transaction_date"), "dd/MM/yyyy"))
    transactions_df = transactions_df.withColumn("transaction_date", to_date(col("transaction_date"), "dd/MM/yyyy"))
    # transactions_df.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy("transaction_id").saveAsTable("transactions_clean")
    return transactions_df

# COMMAND ----------

from pyspark.sql.functions import col, when

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned loans, ingested from Bronze",
  partition_cols=["loan_id"],
  table_properties={
    "WeTrust_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_or_drop("valid loan_id", "loan_id IS NOT NULL")
@dlt.expect_or_drop("valid customer_id", "customer_id IS NOT NULL")

def loans_clean():
    """
    Clean and Normalize Loans DataFrame

    This function takes an input DataFrame `loans_df`, renames its columns to lowercase, converts the "purpose" column to lowercase, and replaces extreme values in the "current_loan_amount" column for data consistency.

    Parameters:
    - loans_df (DataFrame): The input DataFrame containing loan data.

    Returns:
    - DataFrame: A cleaned and normalized DataFrame with lowercase column names, lowercase "purpose" values, and corrected "current_loan_amount" values.

    """
    loans_df = dlt.read('loans_raw')
    loans_df = loans_df.select([col(column).alias(column.lower()) for column in loans_df.columns])
    test_df= loans_df.withColumn("Purpose",when(col("Current_Loan_Amount")=="99999999", "Buisness funding").otherwise(col("Purpose")))
    loans_df = loans_df.withColumn('Purpose', when(loans_df['Purpose'] =='other','Other').otherwise(loans_df['Purpose']))
    loans_df = loans_df.fillna({'Purpose':'Other'})
    loans_df= test_df.fillna(0)
    loans_df = loans_df.withColumn("loan_sanctioned_date", date_format(col("loan_sanctioned_date"), "dd/MM/yyyy"))
    loans_df = loans_df.withColumn("loan_sanctioned_date", to_date(col("loan_sanctioned_date"), "dd/MM/yyyy"))
    # loans_df.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy("loan_id").saveAsTable("loans_clean")
    return loans_df

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned credit, ingested from Bronze",
  partition_cols=["customer_id"],
  table_properties={
    "WeTrust_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_or_drop("valid customer_id", "customer_id IS NOT NULL")
def credit_clean():
    """
    Clean and Normalize Credit DataFrame

    This function takes an input DataFrame `credit_df`, renames its columns to lowercase, and adjusts the "credit_score" values by dividing scores greater than 1000 by 10 for data consistency.

    Parameters:
    - credit_df (DataFrame): The input DataFrame containing credit data.

    Returns:
    - DataFrame: A cleaned and normalized DataFrame with lowercase column names and adjusted "credit_score" values.
    """
    credits_df = dlt.read('credits_raw')
    credits_df = credits_df.select([col(column).alias(column.lower()) for column in credits_df.columns])
    credits_df= credits_df.fillna("0")
    credits_df = credits_df.withColumn(
    "credit_score",
    when((credits_df["credit_score"] > 850), credits_df["credit_score"] / 10)
    .when((credits_df["credit_score"] == 0), 300)
    .otherwise(credits_df["credit_score"])
)
    # credits_df.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy("customer_id").saveAsTable("credits_clean")
    return credits_df

# COMMAND ----------


