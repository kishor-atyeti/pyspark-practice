from pyspark.sql.session import SparkSession
from lib.logger import Log4J

if __name__ == "__main__":



    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("HelloSparkSQL1") \
        .getOrCreate()

    #.config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties") \
    #.config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties") \

    logger = Log4J(spark)

    logger.info("First Message Logged")

    visaPQDF = spark.read \
        .format("csv") \
        .load("customers-100.csv")

    # visaPQDF.show(5)
    # print(visaPQDF.schema.simpleString())

    visaPQDF.write \
        .format('avro') \
        .mode('overwrite') \
        .option('path', 'data/avro/') \
        .save()