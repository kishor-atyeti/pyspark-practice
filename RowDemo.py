from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

def to_date_df(df, fmt, fld):
    return df.withColumn(fld, to_date(col(fld), fmt))

spark = SparkSession.builder \
        .appName("Main Spark") \
        .master("local[3]") \
        .getOrCreate()

mySchema = StructType([
    StructField("ID", StringType()),
    StructField("EventDate", StringType())
])

if __name__ == "__main__":

    myRows = [Row('124', "04/05/2020"), Row('125', "04/5/2020"), Row('126', "4/05/2020"), Row('127', '4/5/2020')]
    myRdd = spark.sparkContext.parallelize(myRows)
    myDF = spark.createDataFrame(myRdd, mySchema)

    #myDF.printSchema()
    myDF.show()
    newDF = to_date_df(myDF, "M/d/y", "EventDate")
    #newDF.printSchema()
    newDF.show()

    # Add 1 day to the date
    dateDF = newDF.withColumn("EventDate", expr("date_add(EventDate, 1)"))
    dateDF.show()