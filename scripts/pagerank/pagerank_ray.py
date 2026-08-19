import pandas as pd
import pyarrow as pa
import ray
import numpy as np
from ray.data import Dataset, from_arrow, from_pandas
from ray.data.aggregate import AggregateFn
from functools import partial
from typing import Iterable, Tuple
from pyarrow import fs, csv
import time
import sys

start_time = time.time()

parse_options = csv.ParseOptions(delimiter="\t")
read_options = csv.ReadOptions(column_names=["src", "dst"])
num_partitions = 20

ray.init(address="auto")

hdfs_fs = fs.HadoopFileSystem.from_uri("hdfs://okeanos-master:54310")

file_path = sys.argv[1]
ds = ray.data.read_csv(file_path, filesystem=hdfs_fs, parse_options=parse_options, read_options=read_options)
#ds = ds.repartition(num_partitions)

def build_adjacency_list(batch):
    adj_list = {}
    for src, dst in zip(batch["src"], batch["dst"]):
        if src not in adj_list:
            adj_list[src] = []
        adj_list[src].append(dst)
    return {"adj_list": adj_list}

def extract_nodes(batch):
    nodes = set(batch["src"]) | set(batch["dst"])  # Unique nodes
    return {"node": np.array(list(nodes), dtype=np.int64)}

ds_adj = ds.map_batches(build_adjacency_list)

ds_nodes = ds.map_batches(extract_nodes)
ds_nodes = ds_nodes.map_batches(lambda batch: {"node": batch["node"]})
ds_nodes = ds_nodes.groupby("node").count().map(lambda x: {"node": x["node"]})

num_nodes = ds_nodes.count()
initial_rank = 1.0 / num_nodes
ds_ranks = ds_nodes.map(lambda node: {"node": node, "rank": initial_rank})

DAMPING = 0.85
NUM_ITERATIONS = 10

def compute_contributions(batch):
    rank_dict = {entry["node"]: entry["rank"] for entry in batch}
    for entry in batch:
        src = entry["node"]
        if "adj_list" in entry and entry["adj_list"]:
            neighbors = entry["adj_list"]
            contribution = rank_dict.get(src, 0) / len(neighbors)
            for dst in neighbors:
                yield {"node": dst, "contribution": contribution}

adj_dict = {entry["node"]: list(entry["adj_list"]) for entry in ds_adj.take(ds_adj.count())}

def merge_rank_and_adj(batch):
    nodes, ranks, adj_lists = [], [], []
    for entry in batch:
        node = entry["node"]
        rank = entry["rank"]
        adj_list = adj_dict.get(node, [])
        nodes.append(node)
        ranks.append(rank)
        adj_lists.append(adj_list)
    return {"node": np.array(nodes), "rank": np.array(ranks), "adj_list": np.array(adj_lists, dtype=object)}

for i in range(NUM_ITERATIONS):
    ds_ranks = ds_ranks.map(lambda x: {"node": x["node"], "rank": x["rank"]})
    ds_ranks = ds_ranks.map_batches(merge_rank_and_adj)
    ds_contribs = ds_ranks.flat_map_batches(compute_contributions)
    ds_ranks = (
        ds_contribs.groupby("node")
        .sum("contribution")
        .map(lambda x: {"node": x["node"], "rank": (1 - DAMPING) / num_nodes + DAMPING * x["contribution"]})
    )

top_20 = ds_ranks.sort(key="rank", descending=True).take(20)
print("\nTop 10 Nodes by PageRank:")
for entry in top_20:
    print(f"Node {entry['node']}: {entry['rank']:.6f}")

end_time = time.time()
print("Stats:", ds.stats())
print(f"Total runtime: {end_time - start_time} seconds")
