# Databricks notebook source
#mount adbf to adls
spark.conf.set(
    "fs.azure.account.key.***",
    "***"
)

# COMMAND ----------





# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# ✅ Initialize Spark Session
spark = SparkSession.builder \
    .appName("ConfigSetup") \
    .getOrCreate()

# ✅ Define Config Schema
config_schema = StructType([
    StructField("SettingName", StringType(), False),
    StructField("SettingValue", StringType(), False)
])

# ✅ Define Config Data
config_data = [
    ("BronzeRawPath", "abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/raw/"),
    ("BronzeStagePath", "abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/stage/"),
    ("ArchivePath", "abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/archive/"),
    ("RejectPath", "abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/reject/"),
    ("SilverPath", "abfss://silver@tg00ginats24tamperdadls.dfs.core.windows.net/"),
    ("ExceptionPath", "abfss://silver@tg00ginats24tamperdadls.dfs.core.windows.net/exception"),
    ("LogPath", "abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/logs/"),
    ("GoldPath","abfss://gold@tg00ginats24tamperdadls.dfs.core.windows.net/")

]

# ✅ Convert to DataFrame
df_config = spark.createDataFrame(config_data, schema=config_schema)

# ✅ Define Config CSV Path
config_csv_path = "abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/config/"

# ✅ Save as CSV
df_config.coalesce(1).write.mode("overwrite").option("header", "true").csv(config_csv_path)

print("✅ Config CSV Created Successfully!")

# COMMAND ----------

# Read the configuration table in any notebook
config_df = spark.read.format("csv").option("header","true").schema(config_schema).load(config_csv_path)

# Create a dictionary of the setting names and values for easy access
config_dict = dict(config_df.rdd.map(lambda row: (row['SettingName'], row['SettingValue'])).collect())

# Access values based on SettingName (e.g., "raw_path", "stage_path")
raw_path = config_dict.get("BronzeRawPath")
stage_path = config_dict.get("BronzeStagePath")
archive_path = config_dict.get("ArchivePath")
reject_path = config_dict.get("RejectPath")
silver_path = config_dict.get("SilverPath")
gold_path = config_dict.get("GoldPath")
log_path = config_dict.get("LogPath")
exception_path=config_dict.get("ExceptionPath")

# Print to verify
print("Raw Path:", raw_path)
print("Stage Path:", stage_path)
print("Archive Path:", archive_path)
print("Reject Path:", reject_path)
print("Silver Path:", silver_path)
print("Gold Path:", gold_path)
print("Log Path:", log_path)
print("Exception Path:", exception_path)
