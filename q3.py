import sys
from pyspark.sql import SparkSession

# you may add more import if you need to
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import col, avg, lit

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW
schema = StructType([
    StructField("_c0", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Cuisine Style", StringType(), True),
    StructField("Ranking", StringType(), True), 
    StructField("Rating", FloatType(), True),  
    StructField("Price Range", StringType(), True),
    StructField("Number of Reviews", StringType(), True), 
    StructField("Reviews", StringType(), True),
    StructField("URL_TA", StringType(), True),
    StructField("ID_TA", StringType(), True)
])
df = spark.read.option("header", True).schema(schema).csv(f"hdfs://{hdfs_nn}:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv")
df_city_avg = df.groupBy("City").agg(avg("Rating").alias("AverageRating"))

tops = df_city_avg.orderBy(col("AverageRating").desc()).limit(3).withColumn("RatingGroup", lit("Top"))
bottoms = df_city_avg.orderBy(col("AverageRating").asc()).limit(3).withColumn("RatingGroup", lit("Bottom"))


combined = tops.union(bottoms).orderBy(col("AverageRating").desc())
# collect = combined.collect()
# print(collect[:6])
output_path = f"hdfs://{hdfs_nn}:9000/assignment2/output/question3/"
combined.write.mode("overwrite").option("header", "true").csv(output_path)
