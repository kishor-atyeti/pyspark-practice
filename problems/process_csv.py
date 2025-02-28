from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = SparkSession.builder \
    .appName("CalculateRevenue") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("mode", "FAILFAST") \
        .load("revenue.csv")

df = df.withColumn("revenue", col("quantity") * col("price"))
total_revenue_df = df.groupBy("product").agg(sum("revenue").alias("total_revenue"))

max_revenue_product = total_revenue_df.orderBy(col("total_revenue").desc()).first()

total_revenue_df.show()
print(f"Product with the highest revenue: {max_revenue_product['product']} with revenue {max_revenue_product['total_revenue']}")