from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from sparkmeasure import TaskMetrics, StageMetrics
from pyspark.sql.functions import col, sum, count, split
from pyspark.sql.types import StructType, StructField, StringType
import os
from pyspark.sql.functions import sum as spark_sum
import sys
import time
from graphframes import *

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sparky = SparkSession \
    .builder \
    .appName("triangle counting") \
    .master("yarn") \
    .config("spark.executor.instances", sys.argv[1]) \
    .config("spark.jars.packages", "ch.cern.sparkmeasure:spark-measure_2.12:0.23,graphframes:graphframes:0.8.3-spark3.5-s_2.12") \
    .getOrCreate() \

sc = sparky.sparkContext

stagemetrics = StageMetrics(sparky)
stagemetrics.begin()

schema = StructType([
    StructField("src", StringType(), True),
    StructField("dst", StringType(), True)
])


edges_df = sparky.read.format("csv") \
    .option("header", "false") \
    .option("delimiter", "\t") \
    .schema(schema) \
    .load("hdfs://okeanos-master:54310"+sys.argv[2])

#
vertices_df = edges_df \
              .select("src") \
              .union(edges_df.select("dst")) \
              .distinct() \
              .withColumnRenamed('src', 'id') \
              #.withColumn('name', col('id').cast('string'))


#edges_df.show()
#vertices_df.show()

graph=GraphFrame(vertices_df,edges_df)
triangle_df = graph.triangleCount()
total_triangles = triangle_df.select(spark_sum("count")).collect()[0][0] // 3

print(f"Number of triangles in the graph: {total_triangles}")


stagemetrics.end()
stagemetrics.print_report()
print(stagemetrics.aggregate_stagemetrics())

patience = 20
while patience > 0:
    try:
        stagemetrics.print_memory_report()
        patience = -1
        print("memory report printed")
    except:
        print("memory report not ready")
        time.sleep(1)
        patience -= 1
print("memory report never ready :(")
sc.stop()
