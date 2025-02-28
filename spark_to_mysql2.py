from pyspark.sql import SparkSession
from faker import Faker
from lib.SparkMySQLModel import SparkMySQLModel

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("HelloSparkSQL") \
        .config("spark.jars", "C:\\spark\\spark-3.5.3-bin-hadoop3\\jars\\mysql-connector-j-8.0.33.jar") \
        .config("spark.executor.extraClassPath",
                "C:\\spark\\spark-3.5.3-bin-hadoop3\\jars\\mysql-connector-j-8.0.33.jar") \
        .getOrCreate()

    fake = Faker()

    def fake_entry():
        full_name = fake.name()
        split_name = full_name.split()
        return full_name, split_name[0], split_name[1], fake.email(), fake.ssn(), fake.job(), abs(2016 - fake.date_time().year) + 1

    def repeat(times, func, *args, **kwargs):
        for _ in range(times):
            yield func(*args, **kwargs)

    person_data = list(repeat(500, fake_entry))
    print(len(person_data))

    table_schema = ["name", "first_name", "last_name", "email_id", "ssn", "occupation", "age"]
    new_df_record = spark.createDataFrame(person_data, table_schema)
    new_df_record.show()

    spark_mysql_model = SparkMySQLModel(spark)
    spark_mysql_model.write_to_mysql(new_df_record, table="person2")

    # --- Reading from Database ----- #
    post_df = spark_mysql_model.read_from_mysql(table="person2")

    post_df.show()

    #post_df.select("name", "email").show(2)
    #post_df.select("name", "phone_num", "email").where("phone_num like '%888%'").show()
    #post_df.filter(col("email").contains("gmail")).show()
