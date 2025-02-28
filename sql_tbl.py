from pyspark.sql import *
from lib.logger import Log4J
import shutil

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Main Spark") \
        .master("local[3]") \
        .getOrCreate()

    logger = Log4J(spark)

    userParqueDF = spark.read \
        .format("parquet") \
        .load("customers-100.parquet")

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")
    #spark.sql("DROP TABLE IF EXISTS user_data_tbl")
    file_path = "spark-warehouse//airline_db.db//user_data_tbl"
    shutil.rmtree(file_path)

    userParqueDF.write \
        .format("csv") \
        .mode("overwrite") \
        .bucketBy(5, "Country", "City") \
        .saveAsTable("user_data_tbl")
    # .option("path", "C://Users//KishorMali//SparkProject//SparkNew//spark-warehouse//airline_db") \
    # .partitionBy("Country", "City") \

    logger.info(spark.catalog.listTables("AIRLINE_DB"))