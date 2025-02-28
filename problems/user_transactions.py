from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum

spark = SparkSession.builder \
        .appName("JoinAndAggregate") \
        .master("local[*]") \
        .getOrCreate()

users_data = [
    (1, "Kishor", 30),
    (2, "Pralhad", 25),
    (3, "Quazi", 35)
]

transactions_data = [
    (1001, 1, 50.0),
    (1002, 1, 30.0),
    (1003, 2, 20.0),
    (1004, 3, 70.0),
    (1005, 3, 40.0)
]

users_columns = ["user_id", "name", "age"]
transactions_columns = ["transaction_id", "user_id", "amount"]

users_df = spark.createDataFrame(users_data, users_columns)
#users_df.show()
transactions_df = spark.createDataFrame(transactions_data, transactions_columns)
#transactions_df.show()

user_trans_df = users_df.join(transactions_df, on="user_id")
#user_trans_df.show()

total_amount_df = user_trans_df.groupBy("user_id", "name").agg(_sum("amount").alias("total_amount"))
total_amount_df.show()