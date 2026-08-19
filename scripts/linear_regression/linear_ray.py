import ray
from ray.data import read_csv
from ray.train import ScalingConfig
from sklearn.linear_model import SGDRegressor
import sys
import time
import numpy as np
from pyarrow import fs
from sklearn.metrics import mean_squared_error, r2_score

start_time = time.time()

ray.init(address="auto")

hdfs_fs = fs.HadoopFileSystem.from_uri("hdfs://okeanos-master:54310")
file_path = sys.argv[1]  

selected_features = ['feature_1', 'feature_2', 'feature_3', 'feature_4']
target = "label"

ds = read_csv(file_path, filesystem=hdfs_fs).select_columns(selected_features + [target])

def convert_types(batch):
    return {col: batch[col].astype(np.float64) for col in selected_features + [target]}
ds = ds.map_batches(convert_types)

model = SGDRegressor(max_iter=1, learning_rate="constant", eta0=0.01, warm_start=True)

for batch in ds.iter_batches(batch_size=1024, batch_format="numpy"):
    X_batch = np.column_stack([batch[col] for col in selected_features])
    y_batch = batch[target]
    model.partial_fit(X_batch, y_batch)

eval_df = ds.random_sample(0.01).to_pandas()
X_test = eval_df[selected_features].values
y_test = eval_df[target].values

predictions = model.predict(X_test)

print("\nModel Performance:")
print(f"RMSE: {np.sqrt(mean_squared_error(y_test, predictions))}")
print(f"R² Score: {r2_score(y_test, predictions)}")

print("\nFinal Model Parameters:")
print(f"Weights: {model.coef_}")
print(f"Bias (Intercept): {model.intercept_}")

end_time = time.time()
print(f"Total runtime: {end_time - start_time} seconds")
