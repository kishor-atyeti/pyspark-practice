from pyspark.sql import *

#from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Main Spark") \
        .master("local[3]") \
        .getOrCreate()

    print(spark)

    #logger = Log4J(spark)

    #logger.info("Start")

    #logger.info("End")

    # spark.stop()

