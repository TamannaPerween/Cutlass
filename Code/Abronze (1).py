# Databricks notebook source
#mount adbf to adls
spark.conf.set(
    "fs.azure.account.key.***",
    "***"
)

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./NBLoadLog

# COMMAND ----------


dbutils.fs.ls("abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/raw/dimension /account")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Define schema once
account_schema = StructType([
    StructField("RowId", StringType(), True),
    StructField("AccountNo", StringType(), True),
    StructField("AccountDesc", StringType(), True),
    StructField("ParentRowId", StringType(), True),
    StructField("DateOpened", TimestampType(), True),
    StructField("DateClosed", TimestampType(), True)
])

# Expected Schema (same as account_schema)
expected_schema = account_schema

# Paths
raw_path = config_dict.get('BronzeRawPath') + "dimension /account"
stage_path = config_dict.get('BronzeStagePath') + "dimension/Account"
archive_path = config_dict.get("ArchivePath") + "dimension/Account"
reject_path = config_dict.get("RejectPath") + "dimension/Account"

# Step 1: Monitor the directory for available .csv files
raw_files = dbutils.fs.ls(raw_path)

# Extract suffixes from file names (like 'account', 'accountv1', etc.)
suffixes = [file.name.split(".")[0] for file in raw_files if file.name.endswith(".csv")]

# Step 2: Loop through each file dynamically
for suffix in suffixes:
    full_file_path = f"{raw_path}/{suffix}.csv"

    # Logging
    udfCreateLog("INFO", "Reading CSV File", f"Reading the CSV file from path: {full_file_path}", 0)

    # Read raw file
    df_raw = spark.read.format("csv").option("header", "true").schema(account_schema).load(full_file_path)

    # Write to delta (Stage layer)
    df_raw.write.format("delta").mode("overwrite").save(f"{stage_path}/{suffix}")

    # Log after writing
    udfCreateLog("INFO", "writing in delta successful", f"writing delta file in path: {stage_path}/{suffix}", df_raw.count())

    # Schema Validation
    if df_raw.schema == expected_schema:
        df_raw.write.format("csv").mode("overwrite").option("header", "true").save(archive_path + f"/{suffix}")
        udfCreateLog("INFO", "Schema matches. Data saved to Archive.", f"Raw account data from {full_file_path} saved to archive.", df_raw.count())
        print(f"{suffix}: ✅ Schema matches. Data saved to Archive.")
    else:
        df_raw.write.format("csv").mode("overwrite").option("header", "true").save(reject_path + f"/{suffix}")
        udfCreateLog("ERROR", "Schema mismatch. Data saved to Reject.", f"Raw account data from {full_file_path} has mismatched schema.", df_raw.count())
        print(f"{suffix}: ❌ Schema mismatch. Data saved to Reject.")
