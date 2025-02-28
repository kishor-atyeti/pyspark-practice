class Log4J:
    def __init__(self, spark):
        # log4j = spark._jvm.org.apache.log4j
        self.logger = spark._jvm.org.apache.log4j.Logger.getLogger("SparkLogger")

    def warning(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def debug(self, message):
        self.logger.debug(message)