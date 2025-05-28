# Databricks notebook source
#mount adbf to adls
sspark.conf.set(
    "fs.azure.account.key.***",
    "***"
)


# COMMAND ----------

# MAGIC %run "/Workspace/Users/tamanna.perween@thorogood.com/config"
# MAGIC

# COMMAND ----------

# MAGIC %run ./NBLoadLog

# COMMAND ----------

# MAGIC %run "/Workspace/Users/tamanna.perween@thorogood.com/Abronze"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# Setup config paths
stage_path = config_dict.get('BronzeStagePath') + 'dimension/Account'
silver_path = config_dict.get("SilverPath") + "dimension/account"

# Define suffixes
raw_files = dbutils.fs.ls(stage_path)
suffixes = [file.name.strip("/") for file in raw_files if file.isDir()]


# Define reusable schema
#account_schema = StructType([...])  # Use your current schema here


# COMMAND ----------

def clean_account_desc(df):
    return df.withColumn("AccountDesc", regexp_replace(trim(upper(col("AccountDesc"))), r"\s+", " ")) \
             .withColumn("AccountDesc", regexp_replace(col("AccountDesc"), r"\s*INC\.$", " INC"))

def filter_exception_rows(df):
    return df.filter(
        col('RowId').isNull() | col('AccountNo').isNull() |
        col('AccountDesc').isNull() | col('ParentRowId').isNull() | col('DateOpened').isNull()
    )

def get_child_parent_joined(child_df, parent_df):
    return child_df.alias("child").join(
        parent_df.alias("parent"),
        col("child.ParentRowId") == col("parent.AccountNo"),
        how="left"
    ).select(
        col("child.RowId").alias("SourceSystemId"),
        "child.AccountNo", "child.AccountDesc",
        col("child.ParentRowId").alias("PrimaryGroupNo"),
        when(col("parent.AccountDesc").isNull(), "Unknown").otherwise(col("parent.AccountDesc")).alias("PrimaryGroupDesc"),
        "child.DateOpened", "child.DateClosed"
    )


# COMMAND ----------

final_cleaned_dfs = []

for suffix in suffixes:
    df = spark.read.format("delta").load(f"{stage_path}/{suffix}")
    
    df_clean = clean_account_desc(df)

    parent_df = df_clean.filter(col("ParentRowId").isNull() & col("DateOpened").isNull() & col("DateClosed").isNull())
    child_df = df_clean.subtract(parent_df)
    exceptions = filter_exception_rows(child_df)
    cleaned_child_df = child_df.subtract(exceptions)

    child_parent_joined = get_child_parent_joined(cleaned_child_df, parent_df)
    
    final_cleaned_dfs.append(child_parent_joined)


# COMMAND ----------

display(exceptions)

# COMMAND ----------

from pyspark.sql.functions import lit, when, current_timestamp, hash, col

# Step 1: Mark base_df with initial isActive
base_df = final_cleaned_dfs[0].withColumn("isActive", lit(1))

# Step 2: Merge remaining versions
new_df = final_cleaned_dfs[1].unionByName(final_cleaned_dfs[2]) if len(final_cleaned_dfs) > 2 else final_cleaned_dfs[1]

# Step 3: Detect new accounts
new_accounts = new_df.join(base_df, on="AccountNo", how="left_anti").withColumn("isActive", lit(1))

# Step 4: Detect changed accounts (same AccountNo but different fields)
updated_accounts = new_df.alias("new").join(base_df.alias("old"), on="AccountNo") \
    .filter((col("new.AccountDesc") != col("old.AccountDesc")) | (col("new.DateOpened") != col("old.DateOpened"))) \
    .select("new.*").withColumn("isActive", lit(1))

# Step 5: Deactivate old versions in base_df
updated_account_nos = updated_accounts.select("AccountNo").rdd.flatMap(lambda x: x).collect()

base_df = base_df.withColumn(
    "isActive", when(col("AccountNo").isin(updated_account_nos), lit(0)).otherwise(col("isActive"))
)

# Step 6: Combine everything
final_combined = base_df.unionByName(updated_accounts).unionByName(new_accounts, allowMissingColumns=True)

# Step 7: Final silver transformations
final_silver_df = final_combined.withColumn(
    "AccountStatus", when(col("DateClosed").isNotNull(), lit(0)).otherwise(lit(1))
).withColumn(
    "AccountSkey", hash(col("SourceSystemId")).cast("long")
).withColumn(
    "InsertedDateTime", current_timestamp()
).withColumn(
    "EndDateTime", when(col("AccountStatus") == 0, current_timestamp()))


# COMMAND ----------

display(final_silver_df)

# COMMAND ----------

display(final_combined)

# COMMAND ----------

display(updated_accounts)

# COMMAND ----------

display(base_df)

# COMMAND ----------

display(final_silver_df)

# COMMAND ----------

final_silver_df.coalesce(1).write.format("delta").mode("overwrite").save(silver_path)


# COMMAND ----------

final_silver_df.printSchema()