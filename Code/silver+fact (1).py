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

# MAGIC %run ./factBronze

# COMMAND ----------

# List all .delta directories
files = dbutils.fs.ls("abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/stage/fact/invoice")

# Extract paths to the actual .delta files (directories with .delta extension)
delta_directories = [file.path for file in files if file.path.endswith('.delta/')]

# If there are no .delta directories, print a message
if len(delta_directories) == 0:
    print("No .delta files found.")
else:
    # Loop through each delta directory and load data
    stage_df_list = []
    for dir_path in delta_directories:
        print(f"Loading data from: {dir_path}")
        # Load the Delta data for each directory
        stage_df = spark.read.format("delta").load(dir_path)
        stage_df_list.append(stage_df)

    # If there are dataframes in the list, perform union
    if stage_df_list:
        final_stage_df = stage_df_list[0]
        for df in stage_df_list[1:]:
            final_stage_df = final_stage_df.union(df)
        
        # Perform your transformations and processing on final_stage_df
        print("Data loaded and unioned successfully.")
    else:
        print("No dataframes to union.")


# COMMAND ----------

stage_df.printSchema()

# COMMAND ----------

final_stage_df.printSchema()

# COMMAND ----------

# Read the account and product dimension tables
account_df = spark.read.format("delta").load("abfss://silver@tg00ginats24tamperdadls.dfs.core.windows.net/dimension/account/")
product_df = spark.read.format("delta").load("abfss://silver@tg00ginats24tamperdadls.dfs.core.windows.net/dimension/product/")
time_df = spark.read.format("delta").load("abfss://silver@tg00ginats24tamperdadls.dfs.core.windows.net/dimension/time/")

# Select only AccountSkey from account_df and ProductSkey from product_df
Naccount_df = account_df.select("AccountNo", "AccountSkey")  # Select only AccountNo and AccountSkey
Nproduct_df = product_df.select("SkuCode", "ProductSkey")    # Select only SkuCode and ProductSkey
Ntime_df = time_df.select("Date", "TimeSkey")    # Select only Date and TimeSkey

Ntime_df=Ntime_df.withColumnRenamed("Date","Ntime_Date")

display(Naccount_df)
display(Nproduct_df)
display(Ntime_df)

# COMMAND ----------





# Perform the join on Account and Product DataFrames and select AccountSkey and ProductSkey
final_stage_df = final_stage_df \
    .join(Naccount_df, final_stage_df["Account"] == Naccount_df["AccountNo"], "left") \
    .join(Nproduct_df, final_stage_df["Product"] == Nproduct_df["SkuCode"], "left") \
    .join(Ntime_df, final_stage_df["Date"] == Ntime_df["Ntime_Date"], "left") \
    .withColumnRenamed("AccountSkey", "AccountSkey_account") \
    .withColumnRenamed("ProductSkey", "ProductSkey_product") \
    .withColumnRenamed("TimeSkey", "TimeSkey_time") \




# Show the resulting dataframe to ensure the correct columns
display(final_stage_df)

# COMMAND ----------

from pyspark.sql.functions import col
# Now, select all columns from final_stage_df and only the AccountSkey and ProductSkey
final_stage_df = final_stage_df.select(
    col("`Invoice.Number`").alias('InvoiceNumber'),
    col("`Line.Item`").alias("LineItem"),
    "Date", 
    "Quantity", 
    col("`Gross.Value`").alias("GrossValue"), 
    col("`Net.Value`").alias("NetValue"), 
    col("VAT").alias("VATvalue"), 
    "AccountSkey_account",  # AccountSkey from account_df 
    "ProductSkey_product",
    "TimeSkey_time"
     # ProductSkey from product_df
    # Other necessary columns from final_stage_df
)    
       # Add ProductSkey from product_df
display(final_stage_df)

# COMMAND ----------

final_stage_df.printSchema()

# COMMAND ----------

final_stage_df.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F
final_stage_df = final_stage_df.withColumn("Year", F.year("Date"))

final_stage_df = final_stage_df.withColumn("Month", F.month("Date"))

display(final_stage_df)

# COMMAND ----------

from pyspark.sql.types import TimestampType
from pyspark.sql import functions as F

final_stage_df = final_stage_df.withColumn("InsertedDateTime", F.current_timestamp())

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import LongType

# Assuming final_stage_df is your PySpark DataFrame

# Add a unique FactInvoiceSkey column with BIGINT type
final_stage_df = final_stage_df.withColumn('FactInvoiceSkey', F.monotonically_increasing_id())

# Ensure the datatype is BIGINT (LongType in PySpark corresponds to BIGINT)
final_stage_df = final_stage_df.withColumn('FactInvoiceSkey', final_stage_df['FactInvoiceSkey'].cast(LongType()))

# Show the updated DataFrame
display(final_stage_df)

# COMMAND ----------

display(final_stage_df)

# COMMAND ----------

from pyspark.sql import functions as F

# Concatenate Invoice.Number and Line.Item into a new column 'InvoiceLineItemNumber'
final_stage_df = final_stage_df.withColumn(
    'InvoiceLineItemNumber',
    F.concat(final_stage_df['InvoiceNumber'], final_stage_df['LineItem'])
)

# Select only the necessary columns for writing to Silver layer and cast the data types to float
silver_df = final_stage_df.select(
    F.col('InvoiceNumber'),
    F.col('Quantity'),
    F.col('GrossValue'),
    F.col('NetValue'),
    F.col('VATvalue'),
    'AccountSkey_account',
    'ProductSkey_product',
    'TimeSkey_time',
    'Year',
    'Month',
    'InsertedDateTime',
    'FactInvoiceSkey',
    'InvoiceLineItemNumber'
)

# Show the resulting dataframe for Silver layer
display(silver_df)


# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

# Define the Silver table path
silver_table_path = "abfss://silver@tg00ginats24tamperdadls.dfs.core.windows.net/fact/invoice"

# Load the existing Delta table to check its schema
existing_silver_df = spark.read.format("delta").load(silver_table_path)
print("Existing Delta Table Schema:")
existing_silver_df.printSchema()

# Print the schema of the DataFrame you want to append
print("DataFrame Schema to Append:")
silver_df.printSchema()

# COMMAND ----------

# Define the Silver table path
silver_table_path = "abfss://silver@tg00ginats24tamperdadls.dfs.core.windows.net/fact/invoice"

# Write final_stage_df to the Silver path (mode 'append' to add data)
silver_df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save(silver_table_path)
silver_table_df = spark.read.format("delta").load(silver_table_path)
display(silver_table_df)


# COMMAND ----------

exception_df = silver_df.filter(
    (silver_df.AccountSkey_account.isNull()) | 
    (silver_df.ProductSkey_product.isNull()) |
    (silver_df.TimeSkey_time.isNull())
    
)

# COMMAND ----------

silver_valid_df = silver_df.filter(
    silver_df.AccountSkey_account.isNotNull() & 
    silver_df.ProductSkey_product.isNotNull() &
    silver_df.TimeSkey_time.isNotNull()
    
)

# COMMAND ----------

# Get the partitions for Year and Month (i.e., the existing data for that partition)
partitioned_df = silver_valid_df.select("Year", "Month").distinct()
display(partitioned_df)

# COMMAND ----------

silver_valid_df.printSchema()

# COMMAND ----------

# Loop through each Year-Month partition and refresh the existing data
for row in partitioned_df.collect():
    year = row["Year"]
    month = row["Month"]

    # Get the records for this partition
    partition_data = silver_valid_df.filter(
        (silver_valid_df.Year == year) & 
        (silver_valid_df.Month == month)
    )

    # Check if data for this partition already exists in the Silver table
    existing_partition_df = silver_table_df.filter(
        (silver_table_df.Year == year) & 
        (silver_table_df.Month == month)
    )

    if existing_partition_df.count() > 0:
        # Replace the data for this partition with the new data
        partition_data.write.format("delta").option("mergeSchema", "true").option("overwriteSchema", "true").mode("overwrite").partitionBy("Year", "Month").save(silver_table_path)
    else:
        # If partition doesn't exist, insert new data
        partition_data.write.option("mergeSchema", "true").format("delta").mode("append").partitionBy("Year", "Month").save(silver_table_path)

# Move exception records to an exception table
exception_table_path = "abfss://silver@tg00ginats24tamperdadls.dfs.core.windows.net/exception/invoice"
exception_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(exception_table_path)

# Final message indicating successful update
print("Data has been successfully processed and inserted into Silver table, and exceptions moved to exception table.")

# COMMAND ----------

partition_data.printSchema()