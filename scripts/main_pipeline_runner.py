from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json
from transform_data import transform
from audit_logger import log_audit
from test_data_quality import run_quality_tests

spark = SparkSession.builder.appName("IngestionPipeline").getOrCreate()

# Load configs
with open("../configs/source_config.json") as f: source_cfg = json.load(f)
with open("../configs/target_config.json") as f: target_cfg = json.load(f)
with open("../configs/ingestion_metadata.json") as f: ingestion_meta = json.load(f)

# Extract
query = f"SELECT * FROM {source_cfg['table']}"
if ingestion_meta["load_mode"] == "append":
    # Assume checkpointing is handled elsewhere
    last_loaded_value = "2023-01-01"  # replace with actual logic
    query += f" WHERE {ingestion_meta['incremental_column']} > '{last_loaded_value}'"

jdbc_url = f"jdbc:sqlserver://{source_cfg['connection']['server']};database={source_cfg['connection']['database']}"
properties = {
    "user": source_cfg["connection"]["username"],
    "password": source_cfg["connection"]["password"],
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

df = spark.read.jdbc(url=jdbc_url, table=f"({query}) AS src", properties=properties)

# Audit pre-transformation
log_audit(spark, df, status="extracted", load_type=ingestion_meta["load_mode"])

# Transform
df_transformed = transform(df)

# Audit post-transformation
log_audit(spark, df_transformed, status="transformed", load_type=ingestion_meta["load_mode"])

# Run quality tests
quality_results = run_quality_tests(df_transformed)
print("DQ Results:", quality_results)

# Load to Delta
df_transformed.write.format("delta") \
    .mode(target_cfg["mode"]) \
    .partitionBy(target_cfg["partition_by"]) \
    .save(target_cfg["path"])

# Final audit
log_audit(spark, df_transformed, status="loaded", load_type=ingestion_meta["load_mode"])
