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

# MAGIC %run ./ProductBronze

# COMMAND ----------


#silver_path=config_dict.get('SilverPath')+'dimension/product'

#df_stage.coalesce(1).write.format("delta").mode("overwrite").save(silver_path)

#df_silver = spark.read.format("delta").load(silver_path)

#display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## product sku

# COMMAND ----------

# Define the function to get distinct level
def get_distinct_level(df_stage, level):
    # Log before filtering data for a specific level
    udfCreateLog(
        logType="INFO",
        logMessage="Start of Level Data Extraction",
        logMessageDetails=f"Filtering Product_Level for {level} and ordering by Product_Desc.",
        rowCount=df_stage.count()  # Row count before applying the filter
    )
    
    # Filter the data based on the Product_Level
    df_filtered = df_stage.filter(df_stage.Product_Level == level).select('*').orderBy("Product_Desc")

    # Log after filtering data for the specific level
    udfCreateLog(
        logType="INFO",
        logMessage=f"Completed Level Data Extraction for {level}",
        logMessageDetails=f"Successfully filtered Product_Level for {level} and ordered by Product_Desc.",
        rowCount=df_filtered.count()  # Row count after applying the filter
    )
    
    return df_filtered

# Get the Product SKU data
product_sku_df = get_distinct_level(df_stage, "Product_SKU")

# Show Product SKU data
display(product_sku_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## exception

# COMMAND ----------

from pyspark.sql.functions import col, lit, when

# Log before filtering exception rows
udfCreateLog(
    logType="INFO",
    logMessage="Start of Exception Rows Filter",
    logMessageDetails="Filtering rows where Product_Desc, Product_Code, or Par_row_Id are null or Par_Row_Id is '1-3Z79-134'.",
    rowCount=product_sku_df.count()  # Row count before applying the filter
)

# Step 1: Filter rows from df_stage where Product_Level is null
exception_df_stage = df_stage.filter(df_stage['Product_Level'].isNull())

# Add the 'exception_reason' column for missing 'Product_Level'
exception_df_stage = exception_df_stage.withColumn(
    "exception_reason", lit("Product_Level is missing")
)

# Step 2: Filter rows from sku_df where any of Par_Row_Id, Product_Desc, Product_Code are null
exception_df_sku = product_sku_df.filter(
    col('Product_Desc').isNull() |
    col('Product_Code').isNull() |
    col('Product_Level').isNull() |
    col('Par_Row_Id').isNull() |
    (col('Par_Row_Id') == lit('1-3Z79-134'))  # You mentioned a specific '1-3Z79-134' value as a check
)

# Add the 'exception_reason' column for each case in sku_df
exception_df_sku = exception_df_sku.withColumn(
    "exception_reason", 
    when(col('Product_Desc').isNull(), lit("Product_Desc is missing"))
    .when(col('Product_Code').isNull(), lit("Product_Code is missing"))
    .when(col('Product_Level').isNull(), lit("Product_Level is missing"))
    .when(col('Par_Row_Id').isNull(), lit("Par_Row_Id is missing"))
    .when(col('Par_Row_Id') == lit('1-3Z79-134'), lit("Not able to achieve next hierarchy"))
    .otherwise(lit("Unknown reason"))
)

# Log after filtering exception rows
udfCreateLog(
    logType="INFO",
    logMessage="Completed Exception Rows Filter",
    logMessageDetails="Filtered exception rows successfully where Product_Desc, Product_Code, or Par_row_Id are null or Par_Row_Id is '1-3Z79-134'.",
    rowCount=product_sku_df.count()  # Row count after filtering
)


# Step 3: Select the same columns from both DataFrames for consistency
exception_df_stage = exception_df_stage.select("*")
exception_df_sku = exception_df_sku.select("*")

# Step 4: Union both DataFrames to combine all the exceptions
exception_df = exception_df_stage.union(exception_df_sku)

# Log after appending the new exception rows to the exception_df
udfCreateLog(
    logType="INFO",
    logMessage="Exception Rows Added to exception_df",
    logMessageDetails="New exception rows have been appended to the existing exception_df.",
    rowCount=exception_df.count()  # Row count after appending
)
exception_df = exception_df.coalesce(1)

# Set the Delta format check configuration to false
spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")

# Step 5: Write the combined exception_df to ADLS in Delta format
exception_df.write.format("delta").mode("overwrite").option("mergeSchema", "true") .save("abfss://silver@tg00ginats24tamperdadls.dfs.core.windows.net/exception/product")

# Step 6: Optionally, you can check the result
display(exception_df)  # Display the rows in the exception DataFrame

print("Exception data written successfully!")


# COMMAND ----------

# Select the same columns from exception_df_sku as in product_sku_df
exception_df_sku_selected = exception_df_sku.select(product_sku_df.columns)

# Subtract exception_df_sku_selected from product_sku_df to get final_sku_df
final_sku_df = product_sku_df.subtract(exception_df_sku_selected)

# Show the result
display(final_sku_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Pack

# COMMAND ----------

from pyspark.sql.functions import col

# Log before performing the join
udfCreateLog(
    logType="INFO",
    logMessage="Start of Join Operation",
    logMessageDetails="Joining final_sku_df with df_stage on matching Par_Row_Id and Source_System_Id.",
    rowCount=final_sku_df.count()  # Row count before join
)

# Assume sku_df has columns: Product_Sku, Par_Row_Id
# Assume stage_df has columns: System_Source_Id, Product_Pack
final_sku_df_alias = final_sku_df.alias("sku")
stage_df_alias = df_stage.alias("stage")

# Perform the join on the matching IDs
pack_df = final_sku_df_alias.join(
    stage_df_alias,
    col("sku.Par_Row_Id") == col("stage.Source_System_Id"),
    how='inner'  # Or 'left', depending on the desired result
)

# Log after performing the join
udfCreateLog(
    logType="INFO",
    logMessage="Completed Join Operation",
    logMessageDetails="Successfully joined final_sku_df with df_stage on matching Par_Row_Id and Source_System_Id.",
    rowCount=pack_df.count()  # Row count after join
)

# Select the relevant columns
pack_df = pack_df.select(
    col("sku.Product_Desc").alias("SkuDesc"),
    col("sku.Product_Code").alias("SkuCode"),
    col("stage.Product_Desc").alias("PackDesc"),
    col("stage.Product_Code").alias("PackCode"),
    col("stage.Par_Row_Id")
)

# Log after selecting the relevant columns
udfCreateLog(
    logType="INFO",
    logMessage="Completed Column Selection",
    logMessageDetails="Successfully selected relevant columns after join.",
    rowCount=pack_df.count()  # Row count after column selection
)

# Show the result
display(pack_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Brand

# COMMAND ----------

from pyspark.sql.functions import col

# Log before performing the join
udfCreateLog(
    logType="INFO",
    logMessage="Start of Join Operation",
    logMessageDetails="Joining pack_df with df_stage on matching Par_Row_Id and Source_System_Id.",
    rowCount=pack_df.count()  # Row count before join
)

# Assume pack_df has columns: Pack_Code, Product_Sku
# Assume stage_df has columns: System_Source_Id, Product_Brand

pack_df_alias = pack_df.alias("pack")
stage_df_alias = df_stage.alias("stage")

# Perform the join on the matching Pack_Code and System_Source_Id
brand_df = pack_df_alias.join(
    stage_df_alias,
    col("pack.Par_Row_Id") == col("stage.Source_System_Id"),
    how='inner'  # Or 'left', depending on the desired result
)

# Log after performing the join
udfCreateLog(
    logType="INFO",
    logMessage="Completed Join Operation",
    logMessageDetails="Successfully joined pack_df with df_stage on matching Par_Row_Id and Source_System_Id.",
    rowCount=brand_df.count()  # Row count after join
)

# Select the relevant columns
brand_df = brand_df.select(
    col("pack.SkuDesc"),
    col("pack.PackDesc"),
    col("pack.SkuCode"),
    col("pack.PackCode"),
    col("stage.Product_Desc").alias("BrandDesc"),
    col("stage.Product_Code").alias("BrandCode"),
    col("stage.Par_Row_Id")
)

# Log after selecting the relevant columns
udfCreateLog(
    logType="INFO",
    logMessage="Completed Column Selection",
    logMessageDetails="Successfully selected relevant columns after join.",
    rowCount=brand_df.count()  # Row count after column selection
)

# Show the result
display(brand_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Product House

# COMMAND ----------

from pyspark.sql.functions import col

# Log before performing the join
udfCreateLog(
    logType="INFO",
    logMessage="Start of Join Operation",
    logMessageDetails="Joining brand_df with df_stage on matching Par_Row_Id and Source_System_Id.",
    rowCount=brand_df.count()  # Row count before join
)

# Assume brand_df has columns: Brand_Code
# Assume stage_df has columns: System_Source_Id, Product_House

brand_df_alias = brand_df.alias("brand")
stage_df_alias = df_stage.alias("stage")

# Perform the join on the matching Brand_Code and System_Source_Id
house_df = brand_df_alias.join(
    stage_df_alias,
    col("brand.Par_Row_Id") == col("stage.Source_System_Id"),
    how='inner'  # Or 'left', depending on the desired result
)

# Log after performing the join
udfCreateLog(
    logType="INFO",
    logMessage="Completed Join Operation",
    logMessageDetails="Successfully joined brand_df with df_stage on matching Par_Row_Id and Source_System_Id.",
    rowCount=house_df.count()  # Row count after join
)

# Select the relevant columns
house_df = house_df.select(
    col("brand.SkuDesc"),
    col("brand.PackDesc"),
    col("brand.SkuCode"),
    col("brand.PackCode"),
    col("brand.BrandDesc"),
    col("brand.BrandCode"),
    col("stage.Product_Desc").alias("HouseDesc"),
    col("stage.Product_Code").alias("HouseCode"),
    col("stage.Par_Row_Id")
)

# Log after selecting the relevant columns
udfCreateLog(
    logType="INFO",
    logMessage="Completed Column Selection",
    logMessageDetails="Successfully selected relevant columns after join.",
    rowCount=house_df.count()  # Row count after column selection
)

# Show the result
display(house_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Group

# COMMAND ----------

from pyspark.sql.functions import col

# Log before performing the join
udfCreateLog(
    logType="INFO",
    logMessage="Start of Join Operation",
    logMessageDetails="Joining house_df with df_stage on matching Par_Row_Id and Source_System_Id.",
    rowCount=house_df.count()  # Row count before join
)

# Assume house_df has columns: House_Code
# Assume stage_df has columns: System_Source_Id, Product_Group

house_df_alias = house_df.alias("house")
stage_df_alias = df_stage.alias("stage")

# Perform the join on the matching House_Code and System_Source_Id
group_df = house_df_alias.join(
    stage_df_alias,
    col("house.Par_Row_Id") == col("stage.Source_System_Id"),
    how='inner'  # Or 'left', depending on the desired result
)

# Log after performing the join
udfCreateLog(
    logType="INFO",
    logMessage="Completed Join Operation",
    logMessageDetails="Successfully joined house_df with df_stage on matching Par_Row_Id and Source_System_Id.",
    rowCount=group_df.count()  # Row count after join
)

# Select the relevant columns
group_df = group_df.select(
    col("house.SkuDesc"),
    col("house.SkuCode"),
    col("house.PackDesc"),
    col("house.PackCode"),
    col("house.BrandDesc"),
    col("house.BrandCode"),
    col("house.HouseDesc"),
    col("house.HouseCode"),
    col("stage.Product_Desc").alias("GroupDesc"),
    col("stage.Product_Code").alias("GroupCode")
)

# Log after selecting the relevant columns
udfCreateLog(
    logType="INFO",
    logMessage="Completed Column Selection",
    logMessageDetails="Successfully selected relevant columns after join.",
    rowCount=group_df.count()  # Row count after column selection
)

# Show the result
display(group_df)


# COMMAND ----------

final_sku_df_renamed = final_sku_df.withColumnRenamed("Product_Desc", "SkuDesc")

# COMMAND ----------

from pyspark.sql.functions import col

# Step 1: group_df ke saare rows le lete hain (pura DataFrame)
group_df_rows = group_df

# Step 2: final_sku_df se required columns ko select karke pre_silver_df mein store karte hain
pre_silver_df = final_sku_df.select(
    col('Product_Desc').alias("SkuDesc"),
    'Traded_Unit',
    'Outers_Per_Traded_Unit',
    'Sticks_Per_Traded_Unit',
    'Packs_Per_Traded_Unit',
    'Grams_Per_Traded_Unit',
    col('Source_System_Id').alias("SourceSystemSKUId")  # Use col function to get the column object
)
pre_silver_df = group_df_rows.join(
    pre_silver_df, 
    on=['SkuDesc'],  # Replace with actual common column name
    how='left'  # You can also use 'inner' or 'outer' depending on the requirement
)



# Show the result in pre_silver_df
display(pre_silver_df)


# COMMAND ----------



from pyspark.sql.functions import hash, col, current_timestamp

# Step 1: Create the 'ProductSKey' column using hash
final_silver_df = pre_silver_df.withColumn(
    "ProductSKey",
    hash(
        col("SourceSystemSKUId"),
        col("Grams_Per_Traded_Unit"),
        col("Packs_Per_Traded_Unit"),
        col("Sticks_Per_Traded_Unit"),
        col("Traded_Unit"),
        col("SkuDesc"),
        col("SkuCode"),
        col("Outers_Per_Traded_Unit")
    ).cast("bigint")
)

# Step 2: Add 'InsertedDateTime' and 'ModifiedDateTime' columns with current timestamp
final_silver_df = final_silver_df.withColumn(
    "InsertedDateTime", current_timestamp()
).withColumn(
    "ModifiedDateTime", current_timestamp()
)

# Show the result (optional)
display(final_silver_df)


# COMMAND ----------

#final_silver_path = "abfss://silver@tg00ginats24tamperdadls.dfs.core.windows.net/dimension/product"
final_silver_path=config_dict.get('SilverPath')+'dimension/product'
from pyspark.sql import functions as F

# Log before writing data to Delta
udfCreateLog(
    logType="INFO",
    logMessage="Start of Write to Final Silver Layer",
    logMessageDetails=f"Writing final_silver_df data to {final_silver_path}.",
    rowCount=final_silver_df.count()  # Row count before writing
)

try:
    # Write the data to the final silver Delta table
    final_silver_df.coalesce(1).write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(final_silver_path)

    # Log after the write operation
    udfCreateLog(
        logType="INFO",
        logMessage="Successful Write to Final Silver Layer",
        logMessageDetails=f"Successfully wrote final_silver_df data to {final_silver_path}.",
        rowCount=final_silver_df.count()  # Row count after writing
    )

except Exception as e:
    # Log any error in case of failure
    udfCreateLog(
        logType="ERROR",
        logMessage="Write to Final Silver Layer Failed",
        logMessageDetails=f"Error occurred while writing to {final_silver_path}: {str(e)}",
        rowCount=0  # Row count is 0 in case of error
    )


# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Step 1: Define the path of the target Delta table (final_silver_table)
#final_silver_table_path = "abfss://silver@tg00ginats24tamperdadls.dfs.core.windows.net/dimension/product"
final_silver_path=config_dict.get('SilverPath')+'dimension/product'
# Log before reading the Delta table
udfCreateLog(
    logType="INFO",
    logMessage="Start of MERGE Operation",
    logMessageDetails=f"Reading and preparing to merge into final_silver table at {final_silver_path}.",
    rowCount=final_silver_df.count()  # Row count from source DataFrame
)

# Step 2: Read the existing Delta table (final_silver_df) as a DeltaTable object
final_silver_delta_table = DeltaTable.forPath(spark, final_silver_path)

# Step 3: Perform the MERGE operation (Upsert)

try:
    final_silver_delta_table.alias("target").merge(
        final_silver_df.alias("source"),  # DataFrame we want to upsert
        "target.ProductSKey = source.ProductSKey"  # Match condition on ProductSKey
    ) \
    .whenMatchedUpdate(
        condition = "target.ProductSKey = source.ProductSKey",  # Update when key matches
        set = {
            "SkuDesc": "source.SkuDesc",  # Update Product Description
            "SourceSystemSKUId": "source.SourceSystemSKUId",
            "Grams_Per_Traded_Unit": "source.Grams_Per_Traded_Unit",
            "Packs_Per_Traded_Unit": "source.Packs_Per_Traded_Unit",
            "Sticks_Per_Traded_Unit": "source.Sticks_Per_Traded_Unit",
            "Traded_Unit": "source.Traded_Unit",
            "SkuCode": "source.SkuCode",
            "Outers_Per_Traded_Unit": "source.Outers_Per_Traded_Unit",
            "InsertedDateTime": "source.InsertedDateTime",
            "ModifiedDateTime": "source.ModifiedDateTime"  # Update Modified Timestamp
        }
    ) \
    .whenNotMatchedInsert(
        values = {
            "ProductSKey": "source.ProductSKey",  # Insert new rows
            "SkuDesc": "source.SkuDesc",
            "SourceSystemSKUId": "source.SourceSystemSKUId",
            "Grams_Per_Traded_Unit": "source.Grams_Per_Traded_Unit",
            "Packs_Per_Traded_Unit": "source.Packs_Per_Traded_Unit",
            "Sticks_Per_Traded_Unit": "source.Sticks_Per_Traded_Unit",
            "Traded_Unit": "source.Traded_Unit",
            "SkuCode": "source.SkuCode",
            "Outers_Per_Traded_Unit": "source.Outers_Per_Traded_Unit",
            "InsertedDateTime": "source.InsertedDateTime",  # Insert Inserted Timestamp
            "ModifiedDateTime": "source.ModifiedDateTime"
        }
    ) \
    .execute()

    # Log after successful merge
    udfCreateLog(
        logType="INFO",
        logMessage="MERGE Operation Completed Successfully",
        logMessageDetails=f"Successfully merged {final_silver_df.count()} rows into final_silver table at {final_silver_path}.",
        rowCount=final_silver_df.count()  # Row count after the merge operation
    )

except Exception as e:
    # Log any error during the merge process
    udfCreateLog(
        logType="ERROR",
        logMessage="MERGE Operation Failed",
        logMessageDetails=f"Error occurred during MERGE operation at {final_silver_path}: {str(e)}",
        rowCount=0  # Row count in case of failure
    )

# Step 4: Optionally, write exception_df if needed for rows with invalid mappings or null values
exception_path = "abfss://silver@tg00ginats24tamperdadls.dfs.core.windows.net/exception/product"

# Log before writing exception data
udfCreateLog(
    logType="INFO",
    logMessage="Start of Write to Exception Layer",
    logMessageDetails=f"Writing exception_df data to {exception_path}.",
    rowCount=exception_df.count()  # Row count before writing
)

try:
    exception_df.write.format("delta").mode("append").save(exception_path)

    # Log after writing exception data
    udfCreateLog(
        logType="INFO",
        logMessage="Successful Write to Exception Layer",
        logMessageDetails=f"Successfully wrote exception_df data to {exception_path}.",
        rowCount=exception_df.count()  # Row count after writing
    )

except Exception as e:
    # Log any error in case of failure during writing exception data
    udfCreateLog(
        logType="ERROR",
        logMessage="Write to Exception Layer Failed",
        logMessageDetails=f"Error occurred while writing to {exception_path}: {str(e)}",
        rowCount=0  # Row count in case of error
    )

# Display the final updated DataFrame (optional)
display(final_silver_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## silver layer finish here 