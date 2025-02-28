# UDF vs Spark function
from faker import Faker
from pyspark.sql import SparkSession
fake = Faker()

spark = SparkSession.builder \
        .appName("Faker") \
        .master("local[*]") \
        .getOrCreate()

# Each entry consists of last_name, first_name, ssn, job, and age (at least 1)
from pyspark.sql import Row
def fake_entry():
  name = fake.name().split()
  return name[1], name[0], fake.ssn(), fake.job(), abs(2016 - fake.date_time().year) + 1

print(fake_entry())

# Create a helper function to call a function repeatedly
def repeat(times, func, *args, **kwargs):
    for _ in range(times):
        yield func(*args, **kwargs)
data = list(repeat(500, fake_entry))
print(len(data))

dataDF = spark.createDataFrame(data, ('last_name', 'first_name', 'ssn', 'occupation', 'age'))
dataDF.show()