# Databricks notebook source
#mount adbf to adls
spark.conf.set(
    "fs.azure.account.key.***",
    "***"
)


# COMMAND ----------

# Define widgets to accept parameters from ADF
dbutils.widgets.text("logType", "INFO")  # Default value: "INFO"
dbutils.widgets.text("logMessage", "Test Log Entry")  # Default value: "Test Log Entry"
dbutils.widgets.text("logMessageDetails", "Details of log entry")  # Default value
dbutils.widgets.text("rowCount", "1000")  # Default value: 1000

# Retrieve the parameters passed from ADF
logType = dbutils.widgets.get("logType")
logMessage = dbutils.widgets.get("logMessage")
logMessageDetails = dbutils.widgets.get("logMessageDetails")
rowCount = int(dbutils.widgets.get("rowCount"))  # Convert to integer if necessary


# COMMAND ----------

# log_utilities notebook
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime

# Define Log Schema
log_schema = StructType([
    StructField("LogDateTime", TimestampType(), True),
    StructField("LogType", StringType(), True),
    StructField("LogMessage", StringType(), True),
    StructField("LogMessageDetails", StringType(), True),
    StructField("LogRowCount", IntegerType(), True),
    StructField("LogNotebookPath", StringType(), True),
    StructField("LogClusterName", StringType(), True),
    StructField("LogClusterID", StringType(), True),
    StructField("LogRunID", StringType(), True),
    StructField("LogUserID", StringType(), True)
])

# Define Log Table Path (ADLS Gen2)
log_path = "abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/logs/"

# Function to Log Data
def udfCreateLog(logType, logMessage, logMessageDetails, rowCount):
    """
    Logs processing details into the Log Delta Table in Databricks.
    """
    try:
        # Retrieve Databricks metadata
        varLogNotebookPath = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
        varLogClusterName = spark.conf.get("spark.databricks.clusterUsageTags.clusterName")
        varLogClusterID = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
        varLogUser = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

        # Create DataFrame for logging
        log_data = [(datetime.now(), logType, logMessage, logMessageDetails, rowCount, varLogNotebookPath, varLogClusterName, varLogClusterID, "N/A", varLogUser)]
        df_log = spark.createDataFrame(log_data, schema=log_schema)

  
        df_log.coalesce(1).write.format("delta").mode("overwrite").option("mergeSchema", "true").save(log_path)
        print(f"Log successfully written to {log_path}")
        # Write Log to Delta Table
       # df_log.coalesce(1).write.format("delta").mode("overwrite").option("mergeSchema", "true").save(log_path)

    except Exception as e:
        print(f"Error: Failed to write log entry. Reason: {str(e)}")


# Call the function to log the entry
udfCreateLog(
    logType=logType,
    logMessage=logMessage,
    logMessageDetails=logMessageDetails,
    rowCount=rowCount
)
