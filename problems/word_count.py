from pyspark.sql import SparkSession
import re

def normalize_text(text):
    # Convert to lowercase
    text = text.lower()
    # Remove punctuation using regex
    text = re.sub(r'[^\w\s]', '', text)
    return text

if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName("WordCount") \
        .master("local[*]") \
        .getOrCreate()

    file = spark.sparkContext.textFile("../resources/words.txt")

    counts = (file.flatMap(lambda line: normalize_text(line).split(" "))
              .map(lambda word: (word, 1))
              .reduceByKey(lambda a, b: a + b))

    # Collect the results and print them
    for word1, count in counts.collect():
        print(f"{word1}: {count}")
