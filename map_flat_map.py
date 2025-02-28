from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("map and flatMap") \
        .getOrCreate()

    rdd = spark.sparkContext.parallelize([1,2,3,4])
    result = rdd.map(lambda x: [x, x * x])
    for ele in result.collect():
        print(ele)

    print("flatMap example")

    flatRdd = spark.sparkContext.parallelize([1,2,3,4])
    result2 = flatRdd.flatMap(lambda x: [x, x * x])
    #print(list(result2.collect()))
    for ele in result2.collect():
        print(ele)