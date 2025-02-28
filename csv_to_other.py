import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("HelloSparkSQL") \
        .getOrCreate()

    userStruct = StructType([
        StructField('Index', IntegerType()),
        StructField('Customer Id', StringType()),
        StructField('First Name', StringType()),
        StructField('Last Name', StringType()),
        StructField('Company', StringType()),
        StructField('City', StringType()),
        StructField('Country', StringType()),
        StructField('Phone 1', StringType()),
        StructField('Phone 2', StringType()),
        StructField('Email', StringType()),
        StructField('Subscription Date', DateType()),
        StructField('Website', StringType())
    ])

    userDDL = '''`Index` STRING, `Customer Id` STRING, `First Name` STRING, `Last Name` STRING, Company STRING,
        Country STRING, `Phone 1` STRING, `Phone 2` STRING, `Subscription Date` DATE, Website STRING'''

    visaDF = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(userStruct) \
        .option("mode", "FAILFAST") \
        .load("customers-100.csv")

    #visaDF.show()
    #print(visaDF.schema.simpleString())

    visaJsonDF = spark.read \
        .format("json") \
        .option("multiline", "true") \
        .schema(userDDL) \
        .load("customers-100.json")

    visaJsonDF.show()
    print(visaJsonDF.schema.simpleString())

    visaPQDF = spark.read \
        .format("parquet") \
        .load("customers-100.parquet")

    # visaPQDF.show(5)
    # print(visaPQDF.schema.simpleString())

