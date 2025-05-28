# Databricks notebook source
#mount adbf to adls
spark.conf.set(
    "fs.azure.account.key.***",
    "***"
)


# COMMAND ----------

# MAGIC %run "/Workspace/Users/tamanna.perween@thorogood.com/config"

# COMMAND ----------

# MAGIC %run ./NBLoadLog

# COMMAND ----------

# MAGIC %run "/Workspace/Users/tamanna.perween@thorogood.com/Asilver"

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws

# ✅ 1. Create Gold Layer by Filtering & Adding Required Columns
gold_layer_df = (
    final_silver_df
    .filter(col("isActive") == 1)
    .withColumn("Account", concat_ws("-", col("AccountNo"), col("AccountDesc")))
    .withColumn("AccountGroup", concat_ws("-", col("PrimaryGroupNo"), col("PrimaryGroupDesc")))
    .select(
        "Account", 
        "AccountGroup", 
        "AccountNo", 
        "AccountDesc", 
        "PrimaryGroupNo", 
        "PrimaryGroupDesc", 
        "DateOpened", 
        "DateClosed", 
        "isActive", 
        "AccountStatus"
    )
)

# ✅ 2. Display Schema (optional - useful for debugging)
gold_layer_df.printSchema()

# ✅ 3. Set Gold Layer Path
gold_path = config_dict.get("GoldPath")
full_gold_path = f"{gold_path}Dimension/Account"

# ✅ 4. Log before writing
udfCreateLog(
    logType="INFO",
    logMessage="Gold layer write started",
    logMessageDetails=f"Writing to {full_gold_path}",
    rowCount=gold_layer_df.count()
)

# ✅ 5. Write Gold Layer
gold_layer_df.coalesce(1).write.mode("overwrite").format("parquet").save(full_gold_path)

# ✅ 6. Log after writing
udfCreateLog(
    logType="INFO",
    logMessage="Gold layer write successful",
    logMessageDetails=f"Written to {full_gold_path}",
    rowCount=gold_layer_df.count()
)


# COMMAND ----------

display(gold_layer_df)