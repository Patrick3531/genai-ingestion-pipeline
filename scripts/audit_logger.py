def log_audit(table_name, row_count, status):
    log = {
        "table": table_name,
        "rows": row_count,
        "status": status,
        "timestamp": str(datetime.now())
    }
    spark.createDataFrame([log]).write.format("delta").mode("append").save("/audit/logs")
