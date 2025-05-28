# Databricks notebook source
# mount adbf to adls
spark.conf.set(
    "fs.azure.account.key.***",
    "***"
)


# COMMAND ----------

# MAGIC %run ./config
# MAGIC

# COMMAND ----------

# MAGIC %run "/Workspace/Users/tamanna.perween@thorogood.com/NBLoadLog"
# MAGIC

# COMMAND ----------

# MAGIC %run ./silver+fact

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, year,add_months,concat_ws, sha2
from pyspark.sql.types import StructType, StructField, LongType, StringType, FloatType, IntegerType

# Initialize Spark session
spark = SparkSession.builder.appName("SilverToGoldLayer").getOrCreate()

# Define the Gold layer schema
gold_schema = StructType([
    StructField("FactInvoiceSkey", LongType(), False),
    StructField("AccountSkey", LongType(), True),
    StructField("ProductSkey", LongType(), True),
    StructField("TimeSkey", IntegerType(), True),
    StructField("InvoiceNumber", StringType(), True),
    StructField("InvoiceLineItemNumber", StringType(), True),
    StructField("Quantity", FloatType(), True),
    StructField("GrossValue", FloatType(), True),
    StructField("NetValue", FloatType(), True),
    StructField("VATvalue", FloatType(), True)
])

# Define the Silver layer path (assuming Silver data is stored as Delta format)
silver_path = "abfss://silver@tg00ginats24tamperdadls.dfs.core.windows.net/fact/invoice/"

# Load the data from Silver layer (Delta format)
silver_df = spark.read.format("delta").load(silver_path)

# Display schema to verify the data
silver_df.printSchema()



# COMMAND ----------

# Filter data to only include records from the last 3 years
# Assuming 'Date' is a timestamp or date column in the Silver layer
from pyspark.sql.functions import current_date, add_months, col

three_years_ago = add_months(current_date(), -36)

filtered_df = silver_df.filter(col("InsertedDateTime") >= three_years_ago)

# Map the Silver schema to Gold schema
gold_df = filtered_df.select(
    col("FactInvoiceSkey").cast("long").alias("FactInvoiceSkey"),
    col("AccountSkey_account").alias("AccountSkey").cast("long"),
    col("ProductSkey_product").alias("ProductSkey").cast("long"),
    col("TimeSkey_time").alias("TimeSkey").cast("integer"),
    col("InvoiceNumber"),
    col("InvoiceLineItemNumber"),
    col("Quantity").cast("float"),
    col("GrossValue").cast("float"),
    col("NetValue").cast("float"),
    col("VATvalue").cast("float")
)

    
    
    
expected_columns = [
    "FactInvoiceSkey", "AccountSkey", "ProductSkey","TimeSkey",
    "InvoiceNumber", "InvoiceLineItemNumber", "Quantity",
    "GrossValue", "NetValue", "VATvalue"
]

assert gold_df.columns == expected_columns, "Schema mismatch!"

# Display the transformed Gold DataFrame schema
gold_df.printSchema()

# Define the Gold layer path (target path for the Gold layer)
gold_path = "abfss://gold@tg00ginats24tamperdadls.dfs.core.windows.net/fact/invoice/"

# Write the transformed data to the Gold layer in Delta format
gold_df.write.format("delta").mode("overwrite").save(gold_path)

print("Data successfully transformed from Silver to Gold layer.")


# COMMAND ----------

display(gold_df)