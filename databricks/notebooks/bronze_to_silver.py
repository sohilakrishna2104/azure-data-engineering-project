# Notebook: Bronze to Silver ETL Transformation
# Bronze to Silver Transformation

# Read raw sales CSV from Bronze layer
df = spark.read.csv("/mnt/raw/sales.csv", header=True, inferSchema=True)

# Drop rows with missing values
df_clean = df.dropna()

# Rename columns for consistency
df_clean = df_clean.withColumnRenamed("Amt", "Amount") \
                   .withColumnRenamed("Region", "RegionName")

# Save cleaned data to Silver layer (Delta format)
df_clean.write.format("delta").mode("overwrite").save("/mnt/silver/sales")
