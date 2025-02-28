from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder \
        .appName("PivotAndUnpivot") \
        .master("local[*]") \
        .getOrCreate()
"""
data = [
    (1,"avila","female","HR"),
    (2,"manasi","female","HR"),
    (3,"kishor","male","delivery"),
    (4,"brijesh","male","delivery"),
    (5,"kshitija","female","management"),
    (6,"kiran","female","lead"),
    (7,"sumit","male","qa"),
    (8,"amit","male","ba"),
    (9,"pralhad","male","customer care"),
    (10,"quazi","male","dwh")
]
"""
df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load("resources/users.csv")

df.show()