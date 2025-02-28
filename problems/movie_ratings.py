from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName("MovieRatings") \
        .master("local[*]") \
        .getOrCreate()

    file_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .load("../resources/ratings_small.csv")

    file_df.show()