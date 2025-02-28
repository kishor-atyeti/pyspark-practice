"""
Create a MySQL read and write objects using Spark
"""

class SparkMySQLModel:
    mysql_db_driver_class = "com.mysql.cj.jdbc.Driver"
    host_name = "localhost"
    port_no = "3306"
    username = "root"
    password = "Admin@1234"
    database_name = "flask"

    mysql_jdbc_connection_string = "jdbc:mysql://" + host_name + ":" + port_no + "/" + database_name

    def __init__(self, spark):
        self.spark = spark

    def spark_setup(self):
        read_instance = self.spark.read \
            .format("jdbc") \
            .option("url", SparkMySQLModel.mysql_jdbc_connection_string) \
            .option("driver", SparkMySQLModel.mysql_db_driver_class) \
            .option("user", SparkMySQLModel.username) \
            .option("password", SparkMySQLModel.password)

        return read_instance

    def read_from_mysql(self, table):
        spark_read_instance = self.spark_setup()
        return (spark_read_instance
                .option("dbtable", table)
                .load())

    def write_to_mysql(self, df, table):
        df.write \
            .format("jdbc") \
            .option("url", SparkMySQLModel.mysql_jdbc_connection_string) \
            .option("driver", SparkMySQLModel.mysql_db_driver_class) \
            .option("user", SparkMySQLModel.username) \
            .option("password", SparkMySQLModel.password) \
            .option("dbtable", table) \
            .mode("append") \
            .save()

        return df