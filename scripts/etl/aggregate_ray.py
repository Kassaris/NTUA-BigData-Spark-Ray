import ray
from pyarrow import fs
import sys

ray.init(address="auto")
hdfs_fs = fs.HadoopFileSystem.from_uri("hdfs://okeanos-master:54310")
ds = ray.data.read_csv(sys.argv[1], filesystem=hdfs_fs) \
        .map_batches(lambda batch: batch,batch_size=500)


ds = ds.groupby("categorical_feature_2") \
       .sum("feature_4")

ds.show(limit=5)

print("stats:", ds.stats())
