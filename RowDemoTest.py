from datetime import date
from unittest import TestCase
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from RowDemo import to_date_df


class RowDemoTest(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .master("local[3]") \
            .appName("Row Demo Test") \
            .getOrCreate()

        mySchema = StructType([
            StructField("ID", StringType()),
            StructField("EventDate", StringType())
        ])

        myRows = [Row('124', "04/05/2020"), Row('125', "04/5/2020"), Row('126', "4/05/2020"), Row('127', '4/5/2020')]
        myRdd = cls.spark.sparkContext.parallelize(myRows)
        cls.myDF = cls.spark.createDataFrame(myRdd, mySchema)

    def test_data_type(self):
        rows = to_date_df(self.myDF, "M/d/y", "EventDate").collect()
        for row in rows:
            self.assertIsInstance(row["EventDate"], date(2020, 4, 5))
