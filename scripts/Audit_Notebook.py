# Databricks notebook source
    spark._jsc.hadoopConfiguration().set("fs.azure.account.key.{}.dfs.core.windows.net".format("genaidatadiggersstorage"), "Azure key")
    print("Storage account is configured using account key")

# COMMAND ----------

output_file_path = 'abfss://output@genaidatadiggersstorage.dfs.core.windows.net/' 

# COMMAND ----------

df = spark.read.format("csv").load(output_file_path+"/pipeline_output.csv")

# Basic transformation (example)
df_transformed = df.withColumn("processed_timestamp", current_timestamp())

# COMMAND ----------

from pyspark.sql.functions import col

# Column list to check
columns_to_check = [
    "EMPLOYEE_ID", "FIRST_NAME", "LAST_NAME", "EMAIL", "PHONE_NUMBER",
    "HIRE_DATE", "JOB_ID", "SALARY", "LAST_UPDATED"
]

# Dictionary to store null counts per column
null_counts = {}

for column in columns_to_check:
    null_counts[column] = df_transformed.filter(col(column).isNull()).count()

# Display null counts
for col_name, count in null_counts.items():
    print(f"Nulls in {col_name}: {count}")


# COMMAND ----------

# Define the path to store the Delta table
delta_path = "abfss://<container>@<storage_account>.dfs.core.windows.net/delta/employee_table"

# Write DataFrame to Delta format
df_transformed.write.format("delta").mode("overwrite").save(delta_path)


# COMMAND ----------

# Write DataFrame to Delta format
df_transformed.write.format("delta").mode("overwrite").save(delta_path)

# COMMAND ----------

df_transformed.write.format("delta").mode("overwrite") \
    .partitionBy("JOB_ID") \
    .saveAsTable("employee_partitioned_table")


# COMMAND ----------

df_transformed.write.format("delta").mode("overwrite") \
    .saveAsTable("hr_db.employee_table")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS hr_db;

# COMMAND ----------

spark.read.table("employee_table").show()
