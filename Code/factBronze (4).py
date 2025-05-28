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

source_path="abfss://sourcefiles@tg00ginats24tamperdadls.dfs.core.windows.net/invoice/"
raw_path = "abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/raw/fact/invoice/"
reject_path = "abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/reject/fact/invoice/"
archive_path = "abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/archive/fact/invoice/"


# COMMAND ----------

import os
# Source folder se saari files ko list karna
source_file = [f.name for f in dbutils.fs.ls(source_path)]

# Print file names for verification
print(source_file)


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType

# Schema define karna
schema = StructType([
    StructField("Invoice.Number", StringType(), True),  
    StructField("Line.Item", StringType(), True),  
    StructField("Date", TimestampType(), True), 
    StructField("Account", StringType(), True),
    StructField("Product", StringType(), True), 
    StructField("Quantity", IntegerType(), True), 
    StructField("Gross.Value", DoubleType(), True),
    StructField("Net.Value", DoubleType(), True), 
    StructField("VAT", DoubleType(), True) 
])

# Source aur Raw path specify karein
source_path = "abfss://sourcefiles@tg00ginats24tamperdadls.dfs.core.windows.net/invoice/"
raw_path = "abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/raw/fact/invoice/"

# Source path se files ko list karein
files_in_source = dbutils.fs.ls(source_path)

# File ko process karne ke liye loop chalayein
for file_info in files_in_source:
    file_name = file_info.name  # File ka naam
    
    # Source path se file ka full path banayein
    source_file_path = os.path.join(source_path, file_name)
    
    # Raw path mein file ka destination path banayein (CSV format ke saath)
    raw_file_path = os.path.join(raw_path, file_name)
    
    try:
        # File ko load karein (CSV format mein)
        df = spark.read.format("csv").option("header", "true").schema(schema).load(source_file_path)
        
        # Actual schema ko print karein
        print(f"Schema for {file_name}:")
        df.printSchema()  # This prints the actual schema Spark inferred for the file
        
        # Check if the actual schema matches the defined schema
        if df.schema == schema:
            print(f"File {file_name} ka schema defined schema se match karta hai.")
        else:
            print(f"File {file_name} ka schema defined schema se match nahi karta hai.")
        
        # Data ko Raw path mein CSV format mein save karein
        df.write.format("csv").option("header", "true").mode("overwrite").save(raw_file_path)
        
        print(f"File {file_name} ko Raw path mein successfully load kiya gaya.")
        
    except Exception as e:
        print(f"File {file_name} ko load karte waqt error aayi: {e}")


# COMMAND ----------

from pyspark.sql import SparkSession
import os



# Source aur Raw path specify karein
source_path = "abfss://sourcefiles@tg00ginats24tamperdadls.dfs.core.windows.net/invoice/"
raw_path = "abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/raw/fact/invoice/"

# Source path se files ko list karein
files_in_source = dbutils.fs.ls(source_path)

# File ko process karne ke liye loop chalayein
for file_info in files_in_source:
    file_name = file_info.name  # File ka naam
    
    # Source path se file ka full path banayein
    source_file_path = os.path.join(source_path, file_name)
    
    # Raw path mein file ka destination path banayein (CSV format ke saath)
    raw_file_path = os.path.join(raw_path, file_name)
    
    try:
        # File ko load karein (CSV format mein)
        df = spark.read.format("csv").option("header", "true").schema(schema).load(source_file_path)
        
        # Data ko Raw path mein CSV format mein save karein
        df.write.format("csv").option("header", "true").mode("overwrite").save(raw_file_path)
        
        print(f"File {file_name} ko Raw path mein successfully load kiya gaya.")
        
    except Exception as e:
        print(f"File {file_name} ko load karte waqt error aayi: {e}")


# COMMAND ----------

import shutil
import os
from pyspark.sql import SparkSession


# Spark session banayein
spark = SparkSession.builder.appName("InvoiceFileValidation").getOrCreate()

# Paths define karein
raw_path = "abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/raw/fact/invoice/"
archive_path = "abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/archive/fact/invoice/"
reject_path = "abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/reject/fact/invoice/"


# Raw path se files ko list karein
files_in_raw = dbutils.fs.ls(raw_path)

# Har file ko process karne ke liye loop chalayein
for file_info in files_in_raw:
    file_name = file_info.name  # File ka naam
    
    # Raw path se file ka full path banayein
    raw_file_path = os.path.join(raw_path, file_name)
    
    # Archive aur Reject path ke liye destination banayein
    archive_file_path = os.path.join(archive_path, file_name)
    reject_file_path = os.path.join(reject_path, file_name)
    
    try:
        # File ko load karein (CSV format mein)
        df = spark.read.format("csv").option("header", "true").schema(schema).load(raw_file_path)
        
        # Data types validation
        data_type_mismatch = False
        for field in schema.fields:
            expected_type = field.dataType
            actual_type = df.schema[field.name].dataType
            
            # Agar expected aur actual data type match nahi karte, toh file ko reject karen
            if expected_type != actual_type:
                print(f"File {file_name} mein column {field.name} ka data type mismatch hai. Rejecting file.")
                data_type_mismatch = True
                break
        
        if data_type_mismatch:
            # Agar data type mismatch hai, toh file ko reject folder mein move karen
            df.write.format("csv").option("header", "true").mode("overwrite").save(reject_file_path)
        else:
            # Agar data types sahi hain, toh file ko archive path mein save karen
            df.write.format("csv").option("header", "true").mode("overwrite").save(archive_file_path)
        
        print(f"File {file_name} ko {'reject' if data_type_mismatch else 'archive'} path mein successfully save kiya gaya.")
    
    except Exception as e:
        print(f"File {file_name} ko process karte waqt error aayi: {e}")
        # Agar koi error aaye, toh file ko reject folder mein move karen
        df.write.format("csv").option("header", "true").mode("overwrite").save(reject_file_path)
        print(f"File {file_name} ko error ke baad reject path mein move kar diya gaya.")


# COMMAND ----------

display(dbutils.fs.ls("abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/raw/fact/invoice"))

# COMMAND ----------

from pyspark.sql.functions import col, trim

# Raw path se files ko list karein
files_in_raw = dbutils.fs.ls(raw_path)

# Har file ko process karne ke liye loop chalayein
for file_info in files_in_raw:
    file_name = file_info.name  # File ka naam
    
    # Raw path se file ka full path banayein
    raw_file_path = os.path.join(raw_path, file_name)

    reject_path = "abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/reject/fact/invoice/"
    stage_path = "abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/stage/fact/invoice/"

    # Stage aur Reject path ke liye destination banayein
    stage_file_path = os.path.join(stage_path, file_name.replace(".csv", ".delta"))
    reject_file_path = os.path.join(reject_path, file_name)
    
    try:
        # File ko load karein (CSV format mein)
        df = spark.read.format("csv").option("header", "true").schema(schema).load(raw_file_path)
    

        # Dimension columns mein blank values ki jaanch karein (Account aur Product)
        invalid_rows = df.filter(
            (trim(col("Account")).isNull()) | (trim(col("Account")) == "") | 
            (trim(col("Product")).isNull()) | (trim(col("Product")) == "") | 
            (col("Account") == "NA") | (col("Product") == "NA")
        )
        
        if invalid_rows.count() > 0:
            print(f"File {file_name} mein Account ya Product column mein blank values hain. Rejecting file.")
            
            # Check if the file is a directory or not
            if dbutils.fs.ls(raw_file_path):
                # If the path is a directory, we need to use recurse=True
                dbutils.fs.mv(raw_file_path, reject_file_path, recurse=True)
                print(f"File {file_name} ko reject path mein move kar diya gaya (directory).")
            else:
                # If it's a file, move it normally
                dbutils.fs.mv(raw_file_path, reject_file_path)
                print(f"File {file_name} ko reject path mein move kar diya gaya (file).")
                
        else:
            # Agar blank values nahi hain, toh data ko Delta format mein stage layer mein save karen
            df.write.format("delta").mode("overwrite").save(stage_file_path)
            print(f"File {file_name} ko successfully stage layer mein Delta format mein save kiya gaya.")
    
    except Exception as e:
        print(f"File {file_name} ko process karte waqt error aayi: {e}")
        
        # If the path is a directory, move it with recurse=True
        if dbutils.fs.ls(raw_file_path):
            dbutils.fs.mv(raw_file_path, reject_file_path, recurse=True)
            print(f"File {file_name} ko error ke baad reject path mein move kar diya gaya (directory).")
        else:
            dbutils.fs.mv(raw_file_path, reject_file_path)
            print(f"File {file_name} ko error ke baad reject path mein move kar diya gaya (file).")


# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### bronze layer finish here