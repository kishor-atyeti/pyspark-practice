from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
        .appName("PivotAndUnpivot") \
        .master("local[*]") \
        .getOrCreate()

data = [
    (1, "Kishor", "male", 1000, "IT", 101),
    (2, "Ramesh", "male", 10200, "HR", 120),
    (3, "Suresh", None, 10010, "HR", 130),
    (4, "Kareena", "female", 12000, None, 110),
    (5, "Katrina", "female", 10030, "HR", 105),
    (6, "Aalia", "female", 19393, "IT", 102),
    (7, "Hritik", "male", 10600, "IT", 160)
]
header = ['id', 'name', 'gender', 'salary', 'dept', 'bonus']
df = spark.createDataFrame(data, header)
df.show()

def total_pay(s, b):
    if s is not None and b is not None:
        return s + b
    return 0

calculate_salary = udf(lambda s,b: total_pay(s,b), IntegerType())

df.withColumn("TotalPay", calculate_salary(df.salary, df.bonus)).show()