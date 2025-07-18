def transform(df):
    return df.withColumn("FIRST_NAME", upper(col("FIRST_NAME"))) \
             .withColumn("LAST_NAME", upper(col("LAST_NAME"))) \
             .withColumn("PHONE_NUMBER", trim(col("PHONE_NUMBER"))) \
             .withColumn("SALARY", round(col("SALARY"), 2))
