import networkx as nx
import sys

nodes=int(sys.argv[1])

G = nx.watts_strogatz_graph(nodes*1000, 100, 0.1).to_directed()

nx.write_edgelist(G, "graphSW{}.tsv".format(nodes), delimiter="\t", data=False)
