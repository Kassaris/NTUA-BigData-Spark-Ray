from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from sparkmeasure import TaskMetrics, StageMetrics
import os
import sys
import time
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sparky = SparkSession \
    .builder \
    .appName("load_data") \
    .master("yarn") \
    .config("spark.executor.instances", sys.argv[1]) \
    .config("spark.jars.packages", "ch.cern.sparkmeasure:spark-measure_2.12:0.23") \
    .getOrCreate()


sc = sparky.sparkContext

stagemetrics = StageMetrics(sparky)
stagemetrics.begin()

df = sparky.read.format("csv") \
           .option("header", "true") \
           .option("inferSchema", "true") \
           .load("hdfs://okeanos-master:54310" + sys.argv[2])

df.show(10)

stagemetrics.end()
stagemetrics.print_report()
print(stagemetrics.aggregate_stagemetrics())

patience = 20
while patience > 0:
    try:
        stagemetrics.print_memory_report()
        print("memory_report_printed")
        patience = -1
    except:
        print("memory report not ready")
        time.sleep(1)
        patience -= 1
print("memory report never ready :(")
sc.stop()
