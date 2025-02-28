from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder \
        .appName("JoinAndAggregate") \
        .master("local[*]") \
        .getOrCreate()

data = [
    (1, 101, "2024-12-01 10:00:00"),
    (1, 101, "2024-12-01 10:05:00"),
    (2, 102, "2024-12-01 11:00:00"),
    (2, 102, "2024-12-01 11:10:00"),
    (3, 103, "2024-12-01 12:00:00"),
    (3, 103, "2024-12-01 15:32:00"),
    (3, 103, "2024-12-01 10:05:00"),
    (4, 104, "2024-12-02 11:00:00"),
    (4, 104, "2024-12-03 11:10:00"),
    (4, 104, "2024-12-01 12:00:00")
]

columns = ["user_id", "event_id", "timestamp"]

df = spark.createDataFrame(data, columns)

window_spec = Window.partitionBy("user_id", "event_id").orderBy(col("timestamp").desc())

df_with_row_num = df.withColumn("row_num", row_number().over(window_spec))
df_with_row_num.show()

deduplicated_df = df_with_row_num.filter(col("row_num") == 1).drop("row_num")
deduplicated_df.show()

salary_df = df.withColumn("salary", df.event_id * 33)
salary_df.show()