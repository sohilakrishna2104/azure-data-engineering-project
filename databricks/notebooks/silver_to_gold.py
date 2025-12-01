# Notebook: Silver to Gold aggregation for business-ready data
# Notebook: Silver to Gold ETL Transformation

# Read cleaned Silver layer data
df_silver = spark.read.format("delta").load("/mnt/silver/sales")

# Aggregate sales by RegionName
df_gold = df_silver.groupBy("RegionName").sum("Amount") \
                   .withColumnRenamed("sum(Amount)", "TotalSales")

# Save aggregated data to Gold layer (Delta format)
df_gold.write.format("delta").mode("overwrite").save("/mnt/gold/sales_summary")
