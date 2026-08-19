import ray
import numpy as np
from ray.data import read_csv
from pyarrow import fs
import sys
import time
from pyarrow import csv

start_time = time.time()

ray.init(address="auto")

hdfs_fs = fs.HadoopFileSystem.from_uri("hdfs://okeanos-master:54310")
file_path = sys.argv[1]  # Pass the file path as a command-line argument

columns = ['feature_1','feature_2','feature_3','feature_4','categorical_feature_1','categorical_feature_2','word','label']
selected_features = ['feature_1','feature_2','feature_3','feature_4']  # Only numerical features for K-Means
#read_options=csv.ReadOptions(column_names=columns)

ds = ray.data.read_csv(file_path, filesystem=hdfs_fs).select_columns(selected_features)

ds = ds.map_batches(lambda batch: {col: batch[col].astype(np.float64) for col in selected_features})

K = 5  # Number of clusters
MAX_ITER = 10  # Max iterations

np.random.seed(24)
initial_centroids = ds.take(K)  # Sample K points as initial centroids
centroids = np.array([[row[col] for col in selected_features] for row in initial_centroids])

def assign_clusters(batch, centroids):
    X = np.column_stack([batch[col] for col in selected_features])
    distances = np.linalg.norm(X[:, np.newaxis] - centroids, axis=2)  # Compute distances
    cluster_assignments = np.argmin(distances, axis=1)  # Get closest centroid
    return {"cluster": cluster_assignments, **batch}

def compute_new_centroids(batch):
    cluster_data = {}
    
    for i in range(len(batch["cluster"])):
        cluster_id = batch["cluster"][i]
        if cluster_id not in cluster_data:
            cluster_data[cluster_id] = []
        cluster_data[cluster_id].append([batch[col][i] for col in selected_features])

    # Convert results into arrays
    cluster_ids = np.array(list(cluster_data.keys()), dtype=np.int64)
    new_centroids = np.array([np.mean(points, axis=0) for points in cluster_data.values()])

    return {"cluster_id": cluster_ids, "new_centroids": new_centroids}

for i in range(MAX_ITER):
    ds = ds.map_batches(lambda batch: assign_clusters(batch, centroids))

    new_centroids_list = ds.map_batches(compute_new_centroids).take(K)

    new_centroids = np.zeros((K, len(selected_features)))
    
    for entry in new_centroids_list:
        cluster_ids = np.atleast_1d(entry["cluster_id"])  # Ensure it's an iterable
        new_centroids_values = np.atleast_2d(entry["new_centroids"])  # Ensure it's 2D
        
        for cluster_id, centroid in zip(cluster_ids, new_centroids_values):
            new_centroids[cluster_id] = centroid

    centroids = new_centroids  # Update for next iteration


print("\nFinal Centroids:")
for i, centroid in enumerate(centroids):
    print(f"Cluster {i}: {centroid}")

end_time = time.time()
ds.show(limit=5)
print("stats:", ds.stats())

print(f"Total runtime: {end_time - start_time} seconds")
