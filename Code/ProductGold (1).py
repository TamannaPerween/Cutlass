# Databricks notebook source
#mount adbf to adls
spark.conf.set(
    "fs.azure.account.key.***",
    "***"
)


# COMMAND ----------

# MAGIC %run "/Workspace/Users/tamanna.perween@thorogood.com/NBLoadLog"

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./ProductSilver

# COMMAND ----------

# Define the path for the final silver table

final_silver_path=config_dict.get('SilverPath')+'dimension/product'
# Log before loading the Delta table
udfCreateLog(
    logType="INFO",
    logMessage="Start of Load Final Silver Table",
    logMessageDetails=f"Loading Delta table from {final_silver_path}.",
    rowCount=0  # We don't know row count yet as we're about to load it
)

# Load the final silver table into final_df_silver
try:
    final_silver_df = spark.read.format("delta").load(final_silver_path)
    
    # Log after successfully loading the table
    udfCreateLog(
        logType="INFO",
        logMessage="Successfully Loaded Final Silver Table",
        logMessageDetails=f"Successfully loaded Delta table from {final_silver_path}.",
        rowCount=final_silver_df.count()  # Row count after loading the DataFrame
    )

    # Display the loaded DataFrame
    display(final_silver_df)

except Exception as e:
    # Log any error that occurs while loading the Delta table
    udfCreateLog(
        logType="ERROR",
        logMessage="Load Final Silver Table Failed",
        logMessageDetails=f"Error occurred while loading Delta table from {final_silver_path}: {str(e)}",
        rowCount=0  # No row count if loading fails
    )


# COMMAND ----------

#gold_path = "abfss://gold@tg00ginats24tamperdadls.dfs.core.windows.net/Dimension/Product"
gold_path=config_dict.get('GoldPath')+'Dimension/product'
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType

# Define Gold Schema
gold_schema = StructType([
    StructField("ProductSkey", LongType(), False),  # Changed to LongType
    StructField("SourceSystemSKUId", StringType(), True),
    StructField("SkuCode", StringType(), True),
    StructField("SKU", StringType(), True),
    StructField("PackingCode", StringType(), True),
    StructField("Packing", StringType(), True),
    StructField("BrandCode", StringType(), True),
    StructField("Brand", StringType(), True),
    StructField("HouseCode", StringType(), True),
    StructField("House", StringType(), True),
    StructField("ProductGroupCode", StringType(), True),
    StructField("ProductGroup", StringType(), True),
    StructField("TradedUnit", StringType(), True),
    StructField("OutersPerTradedUnit", IntegerType(), True),
    StructField("PacksPerTradedUnit", IntegerType(), True),
    StructField("SticksPerTradedUnit", IntegerType(), True),
    StructField("GramsPerTradedUnit", FloatType(), True)
])

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws

# Load Silver data into final_df_silver
final_silver_path=config_dict.get('SilverPath')+'dimension/product'
final_silver_df = spark.read.format("delta").load(final_silver_path)

# Perform your transformations
df_gold = final_silver_df.withColumn("SKU", concat_ws("-", col("SkuCode"), col("SkuDesc"))) \
                          .withColumn("Packing", concat_ws("-", col("PackCode"), col("PackDesc"))) \
                          .withColumn("Brand", concat_ws("-", col("BrandCode"), col("BrandDesc"))) \
                          .withColumn("House", concat_ws("-", col("HouseCode"), col("HouseDesc"))) \
                          .withColumn("ProductGroup", concat_ws("-", col("GroupCode"), col("GroupDesc")))

# Show the result
display(df_gold)


 

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws

# Assume df_silver is already loaded or defined
# Example: Loading a Delta table (adjust the path accordingly)
final_silver_df = spark.read.format("delta").load("abfss://silver@tg00ginats24tamperdadls.dfs.core.windows.net/dimension/product")

# Transform df_silver to df_gold (this is the step that defines 'gold_df')
gold_df = final_silver_df.withColumn("SKU", concat_ws("-", col("SkuCode"), col("SkuDesc"))) \
                   .withColumn("Packing", concat_ws("-", col("PackCode"), col("PackDesc"))) \
                   .withColumn("Brand", concat_ws("-", col("BrandCode"), col("BrandDesc"))) \
                   .withColumn("House", concat_ws("-", col("HouseCode"), col("HouseDesc"))) \
                   .withColumn("ProductGroup", concat_ws("-", col("GroupCode"), col("GroupDesc"))) \
                   .select(
                        col("ProductSkey").cast("long"),  # Cast ProductSkey to LongType
                        col("SourceSystemSKUId"),
                        col("SkuCode"),
                        col("SKU"),
                        col("PackCode"),
                        col("Packing"),
                        col("BrandCode"),
                        col("Brand"),
                        col("HouseCode"),
                        col("House"),
                        col("GroupCode"),
                        col("ProductGroup"),
                        col("Traded_Unit"),
                        col("Outers_Per_Traded_Unit"),
                        col("Packs_Per_Traded_Unit"),
                        col("Sticks_Per_Traded_Unit"),
                        col("Grams_Per_Traded_Unit")
                    )

# Before writing, use coalesce to reduce partitions to 1
gold_df = gold_df.coalesce(1)

# Now that gold_df is defined, you can write it to a Parquet file
gold_path = "abfss://gold@tg00ginats24tamperdadls.dfs.core.windows.net/Dimension/Product"
gold_path=config_dict.get('GoldPath')+'Dimension/Product'
# Write the gold_df DataFrame to Parquet format
gold_df.write.format("parquet").mode("overwrite").save(gold_path)

try:
    # Log after successfully loading the table
    udfCreateLog(
        logType="INFO",
        logMessage="Successfully Loaded gold layer",
        logMessageDetails=f"Successfully loaded Delta table from {gold_path}.",
        rowCount=final_silver_df.count()  # Row count after loading the DataFrame
    )
    # Optional: You can check the output by reading it back
    df_gold = spark.read.format("parquet").load(gold_path)
    display(df_gold)

except Exception as e:
    # Log any error that occurs while loading the Delta table
    udfCreateLog(
        logType="ERROR",
        logMessage="Load gold layer Table Failed",
        logMessageDetails=f"Error occurred while loading Parquet table from {gold_path}: {str(e)}",
        rowCount=0  # No row count if loading fails
    )
