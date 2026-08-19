The scripts here perform triangle counting on a given graph is saved in an edge-list TSV file in the hdfs
*hdfs://okeanos-master:54310/data/graphs/<graph_name>*

Spark uses GraphX (Graphframes), and Ray parallelizes the triangles function from NetworkX.
