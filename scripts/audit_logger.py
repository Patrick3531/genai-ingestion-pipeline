from pyspark.sql.functions import lit, current_timestamp

def log_audit(spark, df, status, load_type):
    audit_df = df.selectExpr("count(*) as record_count") \
        .withColumn("status", lit(status)) \
        .withColumn("load_type", lit(load_type)) \
        .withColumn("timestamp", current_timestamp())
    
    audit_df.write.format("delta") \
        .mode("append") \
        .save("abfss://datalake/audit_logs/employee")
