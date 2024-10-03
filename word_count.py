from pyspark.sql import SparkSession
from datetime import datetime, date
from pyspark.sql import Row
from pyspark.sql.functions import *

spark = SparkSession.builder \
        .master("local[*]") \
        .appName("WordCount") \
        .getOrCreate()

inputs = spark.read.csv("C:\\Users\\KARTHIK\\Desktop\\netflix_titles.csv",header="true", inferSchema="true")

filtered = inputs.filter(col("description").isNotNull())

word_count = (filtered.rdd
              .flatMap(lambda row: row["description"].split(" "))
              .map(lambda word: (word,1))
              .reduceByKey(lambda a,b:a+b)
)

sorted_word_count = word_count.sortBy(lambda x: x[1], ascending=False)

for word,count in sorted_word_count.collect():
    print(f"{word} {count}")