from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
import time
import sys
import numpy
from sparkmeasure import TaskMetrics, StageMetrics

start_time = time.time()

sparky = SparkSession \
    .builder \
    .appName("load_data") \
    .master("yarn") \
    .config("spark.executor.instances", sys.argv[1]) \
    .config("spark.jars.packages", "ch.cern.sparkmeasure:spark-measure_2.12:0.23") \
    .getOrCreate()

stagemetrics = StageMetrics(sparky)
stagemetrics.begin()

df = sparky.read.format("csv") \
           .option("header", "true") \
           .option("inferSchema", "true") \
           .load("hdfs://okeanos-master:54310" + sys.argv[2])

selected_features = ['feature_1', 'feature_2', 'feature_3', 'feature_4']

assembler = VectorAssembler(inputCols=selected_features, outputCol="features")
df = assembler.transform(df).select("features")

K = 5  # Number of clusters
MAX_ITER = 10  # Max iterations

kmeans = KMeans().setK(K).setMaxIter(MAX_ITER).setFeaturesCol("features").setPredictionCol("cluster")
model = kmeans.fit(df)

centroids = model.clusterCenters()
print("\nFinal Centroids:")
for i, centroid in enumerate(centroids):
    print(f"Cluster {i}: {centroid}")

df_clustered = model.transform(df)
df_clustered.show(5)

end_time = time.time()
print(f"Total runtime: {end_time - start_time} seconds")
df.show(5)

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
if patience == 0:
    print("memory report never ready :(")

sparky.stop()
