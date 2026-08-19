import ray
import networkx as nx
from pyarrow import fs
from pyarrow import csv
import sys
import time


start_time = time.time()
parse_options = csv.ParseOptions(delimiter="\t")
read_options = csv.ReadOptions(column_names=["src","dst"])
num_partitions=int(sys.argv[1])*4
hdfs_fs = fs.HadoopFileSystem.from_uri("hdfs://okeanos-master:54310")

file_path = sys.argv[2]
ds = ray.data.read_csv(file_path,filesystem=hdfs_fs,parse_options=parse_options,read_options=read_options)#.repartition(num_partitions)
#ds.show(10)
@ray.remote
def count_triangles(edges):
    """Counts triangles in a partition of the graph."""
    G = nx.Graph()
    G.add_edges_from(edges)

    triangle_counts = nx.triangles(G)

    return sum(triangle_counts.values()) // 3

def parse_edges(batch):
    edges=[]
    
    for src, dst in zip(batch['src'], batch['dst']):
        edges.append((src, dst))
    return {"edges":edges}
ds = ds.map_batches(parse_edges)

future_results = [count_triangles.remote(batch["edges"]) for batch in ds.iter_batches()]
results = ray.get(future_results)

triangle_count = sum(results)
print(f"Total number of triangles: {triangle_count}")
end_time = time.time()
print("stats:", ds.stats())
print(f"Total runtime: {end_time - start_time} seconds")

