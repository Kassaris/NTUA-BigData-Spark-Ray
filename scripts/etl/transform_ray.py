import ray
from pyarrow import fs
import sys

def filter_function(batch):
    batch = batch.drop(batch[batch["new_feature"] < batch["word"].str.len()].index)
    return batch

ray.init(address="auto")
hdfs_fs = fs.HadoopFileSystem.from_uri("hdfs://okeanos-master:54310")
ds = ray.data.read_csv(sys.argv[1], filesystem=hdfs_fs) \
        #.map_batches(lambda batch: batch)

ds = ds.add_column("new_feature", lambda df: df["feature_1"] ** 2 + df["feature_2"]**2) \
        .map_batches(filter_function, batch_format="pandas")

ds.show(limit=5)
print(ds.count())
print("stats:", ds.stats())
