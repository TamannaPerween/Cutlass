# Databricks notebook source
# mount adbf to adls
spark.conf.set(
    "fs.azure.account.key.***",
    "***"
)



# COMMAND ----------

dbutils.widgets.text("start_date", "")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, explode, sequence, to_date, date_format, year, month, dayofweek,
    lit, when, concat_ws, lpad, expr
)
from pyspark.sql.types import DateType, IntegerType, BooleanType
import datetime

# Fetch the date entered by user from widget
start_date_str = dbutils.widgets.get("start_date")

# Fallback in case user forgets to provide input
if not start_date_str:
    raise ValueError(" Please provide a valid start date via widget (e.g., '2018-01-01')")
# Get user input start date from widget

start_date = datetime.datetime.strptime(dbutils.widgets.get("start_date"), "%Y-%m-%d").date()
end_date = start_date.replace(year=start_date.year + 10)

# Load existing data if present
try:
    df_existing = spark.read.format("delta").load("abfss://silver@tg00ginats24tamperdadls.dfs.core.windows.net/dimension/time")
    min_existing = df_existing.agg({"Date": "min"}).collect()[0][0]
    max_existing = df_existing.agg({"Date": "max"}).collect()[0][0]
    full_start = min(start_date, min_existing)
    full_end = max(end_date, max_existing)
except:
    df_existing = None
    full_start = start_date
    full_end = end_date

# Generate full date range
df_dates = spark.sql(f"SELECT sequence(to_date('{full_start}'), to_date('{full_end}'), interval 1 day) as date_seq") \
                .withColumn("Date", explode(col("date_seq"))) \
                .drop("date_seq") \
                .withColumn("Date", col("Date").cast("timestamp"))

# Enrich with required columns
df_dimTime = df_dates \
    .withColumn("TimeSkey", date_format(col("Date"), "yyyyMMdd").cast(IntegerType())) \
    .withColumn("DayOfWeek", date_format(col("Date"), "EEEE")) \
    .withColumn("CurrentDay", F.when(F.col("Date") == F.current_date(), F.lit(1)).otherwise(F.lit(0))) \
    .withColumn("WorkingDay", F.when(F.date_format("Date", "u").cast("int") < 6, F.lit(1)).otherwise(F.lit(0))) \
    .withColumn("Year", year(col("Date"))) \
    .withColumn("Month", month(col("Date"))) \
    .withColumn("MonthId", (year(col("Date")) * 100 + month(col("Date"))).cast(IntegerType())) \
    .withColumn("MonthDesc", F.date_format("Date", "MMMM yyyy")) \
    .withColumn("Quarter", expr("ceil(month(Date)/3)")) \
    .withColumn("QuarterId", (year(col("Date")) * 10 + col("Quarter")).cast("smallint")) \
    .withColumn("QuarterDesc", concat_ws("", lit("Q"), col("Quarter"))) \
    .select(
        "TimeSkey", "Date", "DayOfWeek", "CurrentDay", "WorkingDay",
        "MonthId", "MonthDesc", "QuarterId", "QuarterDesc", "Year"
    ).dropDuplicates(["TimeSkey"])

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Write or overwrite Silver layer
df_dimTime.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("abfss://silver@tg00ginats24tamperdadls.dfs.core.windows.net/dimension/time")


# COMMAND ----------

display(df_dimTime)

# COMMAND ----------

df_dimTime.printSchema()

# COMMAND ----------

# Register as view or save to Delta
df_dimTime.createOrReplaceTempView("vw_dimTimeGold")

# Optional: Save as delta table
df_dimTime.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/mnt/gold/dimTime_gold")
