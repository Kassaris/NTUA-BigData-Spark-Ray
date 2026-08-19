import ray
from pyarrow import fs
import sys

ray.init()
hdfs_fs = fs.HadoopFileSystem.from_uri("hdfs://okeanos-master:54310")
ds = ray.data.read_csv(sys.argv[1], filesystem=hdfs_fs)

print(ds.materialize().stats())
