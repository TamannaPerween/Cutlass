# Databricks notebook source
#mount adbf to adls
spark.conf.set(
    "fs.azure.account.key.***",
    "***"
)


# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run "/Workspace/Users/tamanna.perween@thorogood.com/NBLoadLog"

# COMMAND ----------

### raw level schema ###

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,FloatType
 
Product_schema = StructType([
    StructField("Source_System_Id", StringType(), True),
    StructField("Product_Code", StringType(), True),
    StructField("Product_Desc", StringType(), True),
    StructField("Product_Level", StringType(), True),
    StructField("Par_Row_Id", StringType(), True),
    StructField("Traded_Unit", StringType(), True),
    StructField("Sticks_Per_Traded_Unit", IntegerType(), True),
    StructField("Packs_Per_Traded_Unit", IntegerType(), True),
    StructField("Outers_Per_Traded_Unit", IntegerType(), True),
    StructField("Grams_Per_Traded_Unit", FloatType(), True)
])

# COMMAND ----------

#store in raw path 

raw_path=config_dict.get('BronzeRawPath')+'dimension /product/Product.csv'

udfCreateLog(
    logType="INFO",
    logMessage="Start of CSV Load",
    logMessageDetails=f"Loading product data from {raw_path}.",
    rowCount=0  
)

try:
    df_raw = spark.read.format("csv").option("header", "true").schema(Product_schema).load(raw_path)
    display(df_raw)

    # Print the schema 
    df_raw.printSchema()


  # Log the successful load
    udfCreateLog(
        logType="INFO",
        logMessage="Successful CSV Load",
        logMessageDetails=f"Product data loaded successfully from {raw_path}.",
        rowCount=df_raw.count()  # Get the number of rows loaded
    )
except Exception as e:
    # Log any error if CSV loading fails
    udfCreateLog(
        logType="ERROR",
        logMessage="CSV Load Failed",
        logMessageDetails=f"Error occurred while loading product data from {raw_path}: {str(e)}",
        rowCount=0
    )

# COMMAND ----------


stage_path =config_dict.get('BronzeStagePath')+'dimension/Product'

udfCreateLog(
    logType="INFO",
    logMessage="write csv into delta ",
    logMessageDetails=f"writing product data from {stage_path}.",
    rowCount=0  
)

try:
    
     
    df_raw.write.format("delta").mode("overwrite").save(stage_path)

    # Read the Delta table into a DataFrame
    df_stage = spark.read.format("delta").load(stage_path)

    # Display the DataFrame and print schema
    display(df_stage)
    df_stage.printSchema()

    udfCreateLog(
        logType="INFO",
        logMessage="Successful load csv in delta",
        logMessageDetails=f"Product data loaded successfully from {stage_path}.",
        rowCount=df_stage.count()  
    )

except Exception as e:
    # Log any error if CSV loading fails
    udfCreateLog(
        logType="ERROR",
        logMessage="CSV Load Failed",
        logMessageDetails=f"Error occurred while loading product data from {stage_path}: {str(e)}",
        rowCount=0
    )

# COMMAND ----------

# Define the expected schema as a StructType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,FloatType
 
expected_schema = StructType([
    StructField("Source_System_Id", StringType(), True),
    StructField("Product_Code", StringType(), True),
    StructField("Product_Desc", StringType(), True),
    StructField("Product_Level", StringType(), True),
    StructField("Par_Row_Id", StringType(), True),
    StructField("Traded_Unit", StringType(), True),
    StructField("Sticks_Per_Traded_Unit", IntegerType(), True),
    StructField("Packs_Per_Traded_Unit", IntegerType(), True),
    StructField("Outers_Per_Traded_Unit", IntegerType(), True),
    StructField("Grams_Per_Traded_Unit", FloatType(), True)
])

# COMMAND ----------


raw_path=config_dict.get('BronzeRawPath')+'dimension /product/Product.csv'
# Start of the raw data load log
udfCreateLog(
    logType="INFO",
    logMessage="Start of CSV Load",
    logMessageDetails=f"Loading raw product data from {raw_path}.",
    rowCount=0  # Initially, row count is 0 before loading
)
# Read the raw DataFrame 
df_raw = spark.read.format("csv").option("header","true").schema(expected_schema).load(raw_path)

# Check if the schema of raw_df matches the expected schema
if df_raw.schema == expected_schema:
    # If schemas match, write to the archive folder
    archive_path = config_dict.get('ArchivePath')+'dimension/Product'
    df_raw.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(archive_path)

    # Log success for saving to archive
    row_count = df_raw.count()  # Get the row count after saving
    udfCreateLog(
        logType="INFO",
        logMessage="Schema matches. Data saved to Archive.",
        logMessageDetails=f"Raw product data from {raw_path} saved to archive folder.",
        rowCount=row_count
    )
   
    print("Schema matches. Data saved to Archive.")
else:
    # If schemas do not match, write to the reject folder
    reject_path = config_dict.get('RejectPath')+'dimension/Product'
    df_raw.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(reject_path)

    # Log failure for schema mismatch and saving to reject folder
    row_count = df_raw.count()  # Get the row count after saving to reject
    udfCreateLog(
        logType="ERROR",
        logMessage="Schema mismatch. Data saved to Reject.",
        logMessageDetails=f"Raw product data from {raw_path} has mismatched schema and saved to reject folder.",
        rowCount=row_count
    )

    print("Schema does not match. Data saved to Reject.")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze layer finish here