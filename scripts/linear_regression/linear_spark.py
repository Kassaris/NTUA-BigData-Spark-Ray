from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import sys
import time
from sparkmeasure import StageMetrics

start_time = time.time()

sparky = SparkSession \
    .builder \
    .appName("linear_regression") \
    .master("yarn") \
    .config("spark.executor.instances", sys.argv[1]) \
    .config("spark.jars.packages", "ch.cern.sparkmeasure:spark-measure_2.12:0.23") \
    .getOrCreate()

stagemetrics = StageMetrics(sparky)
stagemetrics.begin()

file_path = "hdfs://okeanos-master:54310" + sys.argv[2]
df = sparky.read.format("csv") \
           .option("header", "true") \
           .option("inferSchema", "true") \
           .load(file_path)

selected_features = ['feature_1', 'feature_2', 'feature_3', 'feature_4']
target = "label"

assembler = VectorAssembler(inputCols=selected_features, outputCol="features")
df = assembler.transform(df).select("features", target)

df = df.withColumn(target, df[target].cast("double"))

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

lr = LinearRegression(featuresCol="features", labelCol=target, maxIter=10, regParam=0.01)

model = lr.fit(train_df)

print("\nFinal Model Parameters:")
print(f"Weights: {model.coefficients}")
print(f"Bias (Intercept): {model.intercept}")

predictions = model.transform(test_df)
predictions.show(5)

training_summary = model.summary
print("\nModel Performance:")
print(f"RMSE: {training_summary.rootMeanSquaredError}")
print(f"R² Score: {training_summary.r2}")

end_time = time.time()
print(f"Total runtime: {end_time - start_time} seconds")

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
