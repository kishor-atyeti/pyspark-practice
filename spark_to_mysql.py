import sys
from pyspark.sql import SparkSession
from faker import Faker
from pyspark.sql.functions import col

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("HelloSparkSQL") \
        .config("spark.jars", "C:\\spark\\spark-3.5.3-bin-hadoop3\\jars\\mysql-connector-j-8.0.33.jar") \
        .config("spark.executor.extraClassPath",
                "C:\\spark\\spark-3.5.3-bin-hadoop3\\jars\\mysql-connector-j-8.0.33.jar") \
        .getOrCreate()
    
    mysql_db_driver_class = "com.mysql.cj.jdbc.Driver"
    table_name = "posts"
    host_name = "localhost"
    port_no = "3306"
    username = "root"
    password = "Admin@1234"
    database_name = "flask"

    mysql_jdbc_connection_string = "jdbc:mysql://" + host_name + ":" + port_no + "/" + database_name

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

    new_df_record.write \
        .format("jdbc") \
        .option("url", mysql_jdbc_connection_string) \
        .option("driver", mysql_db_driver_class) \
        .option("dbtable", "person") \
        .option("user", username) \
        .option("password", password) \
        .mode("append") \
        .save()

    # --- Reading from Database ----- #
    post_df = spark.read \
        .format("jdbc") \
        .option("url", mysql_jdbc_connection_string) \
        .option("driver", mysql_db_driver_class) \
        .option("dbtable", "person") \
        .option("user", username) \
        .option("password", password) \
        .load()

    post_df.show()

    #post_df.select("name", "email").show(2)
    #post_df.select("name", "phone_num", "email").where("phone_num like '%888%'").show()
    #post_df.filter(col("email").contains("gmail")).show()
