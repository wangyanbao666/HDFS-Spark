import sys
from pyspark.sql import SparkSession

# you may add more import if you need to
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import col, max, min

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
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
filtered_df = df.filter(
        (col("Price Range") != "") & 
        (col("Price Range").isNotNull())
    ).groupBy(
        col("Price Range"), col("City")
    ).agg(
        max("Rating").alias("Best Rating"),
        min("Rating").alias("Worst Rating")
    )

df_max_min_rating = df.join(
    filtered_df,
    (df.City == filtered_df.City) & (df["Price Range"] == filtered_df["Price Range"]) & ((df.Rating == filtered_df["Best Rating"] | df.Rating == filtered_df["Worst Rating"])),
    "inner"
)

df_max_min_rating = df_max_min_rating.select(
    "_c0", "Name", "City", "Cuisine Style", "Ranking", "Rating", "Price Range", "Number of Reviews", "Reviews", "URL_TA", "ID_TA"
)

collect = df_max_min_rating.collect()
print(collect[:6])
output_path = f"hdfs://{hdfs_nn}:9000/assignment2/output/question2/"
df_max_min_rating.write.mode("overwrite").option("header", "true").csv(output_path)
