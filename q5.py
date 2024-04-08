import sys 
from pyspark.sql import SparkSession
# you may add more import if you need to
from pyspark.sql.functions import col, count, split, explode, regexp_replace, trim, lit, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType, MapType, LongType, IntegerType


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW
schema = StructType([
    StructField("movie_id", LongType(), True),
    StructField("title", StringType(), True),
    StructField("cast", StringType(), True),
    StructField("crew", StringType(), True) 
])

cast_schema = ArrayType(StructType([
    StructField("cast_id", IntegerType(), True),
    StructField("character", StringType(), True),
    StructField("credit_id", StringType(), True),
    StructField("gender", IntegerType(), True),
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("order", IntegerType(), True),
]))
df = spark.read.option("header", True).schema(schema).parquet(f"hdfs://{hdfs_nn}:9000/assignment2/part2/input/tmdb_5000_credits.parquet")
df_with_cast = df.withColumn("cast", from_json("cast", cast_schema))
df_actors = df_with_cast.select("movie_id", "title", explode("cast").alias("actor"))
df_actors = df_actors.select("movie_id", "title", df_actors.actor.getItem("name").alias("actor_name"))
df_actor_pairs = df_actors.alias("a1").join(df_actors.alias("a2"),
                                             on=["movie_id"],
                                             how="inner").where(col("a1.actor_name") < col("a2.actor_name"))

df_actor_pairs = df_actor_pairs.select(col("a1.movie_id"), col("a1.title"),
                                       col("a1.actor_name").alias("actor1"),
                                       col("a2.actor_name").alias("actor2"),
                                       lit(1).alias("count"))

df_actor_pairs_grouped = df_actor_pairs.groupBy("movie_id", "title", "actor1", "actor2").sum("count")

df_actor_pairs_filtered = df_actor_pairs_grouped.filter(col("sum(count)") >= 2).select("movie_id", "title", "actor1", "actor2").orderBy("movie_id")
# collect = df_actor_pairs_filtered.collect()
# print(collect[:6])
output_path = f"hdfs://{hdfs_nn}:9000/assignment2/output/question5/"
df_actor_pairs_filtered.write.mode("overwrite").option("header", "true").parquet(output_path)
