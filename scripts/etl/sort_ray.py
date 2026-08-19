import ray
from pyarrow import fs
import sys


ray.init(address="auto")

ctx = ray.data.DataContext.get_current()
ctx.use_push_based_shuffle = True  # Enables more efficient shuffling
#ctx.execution_options.resource_limits.cpu = max(1, int(sys.argv[1]))  # Limit CPU usage


hdfs_fs = fs.HadoopFileSystem.from_uri("hdfs://okeanos-master:54310")

ds = ray.data.read_csv(sys.argv[1], filesystem=hdfs_fs) \
       .map_batches(lambda batch: batch,batch_size=500)

ds_sorted = ds.sort("feature_2")
ds_sorted.show(limit=5)
print("stats:", ds_sorted.stats())
