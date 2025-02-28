from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Main Spark") \
        .getOrCreate()

    rdd = spark.sparkContext.parallelize([1, 2, 3, 4])
    mapped_rdd = rdd.map(lambda x: x * 2)
    print(mapped_rdd.collect())  # Output: [2, 4, 6]
    # Example of flatMap
    flat_mapped_rdd = rdd.flatMap(lambda x: [x, x * 2])
    print(flat_mapped_rdd.collect())  # Output: [1, 2, 2, 4, 3, 6]