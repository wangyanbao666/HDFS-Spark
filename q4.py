import sys
from pyspark.sql import SparkSession

# you may add more import if you need to
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import col, count, split, explode, regexp_replace, trim

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW
schema = StructType([
    StructField("_c0", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Cuisine Style", StringType(), True),
    StructField("Ranking", StringType(), True),  # Consider IntegerType if appropriate
    StructField("Rating", FloatType(), True),   # Consider FloatType or DoubleType if appropriate
    StructField("Price Range", StringType(), True),
    StructField("Number of Reviews", StringType(), True),  # Consider IntegerType if appropriate
    StructField("Reviews", StringType(), True),
    StructField("URL_TA", StringType(), True),
    StructField("ID_TA", StringType(), True)
])
df = spark.read.option("header", True).schema(schema).csv(f"hdfs://{hdfs_nn}:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv")
df = df.withColumn("Cuisine Style r", regexp_replace(col("Cuisine Style"), "[\[\]]", ""))
df = df.withColumn("Cuisine List", split(col("Cuisine Style r"), ", "))
df = df.withColumn("Cuisine", explode(col("Cuisine List")))
df = df.withColumn("Cuisine", trim(regexp_replace(col("Cuisine"), "'", "")))
df_city_cuisine_count = df.groupBy(col("City"), col("Cuisine")).agg(count("Rating").alias("count"))
collect = df_city_cuisine_count.collect()
print(collect[:6])
output_path = f"hdfs://{hdfs_nn}:9000/assignment2/output/question4/"
df_city_cuisine_count.write.mode("overwrite").option("header", "true").csv(output_path)
