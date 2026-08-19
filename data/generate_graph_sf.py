import networkx as nx
import sys

nodes=int(sys.argv[1])

m=5

G= nx.barabasi_albert_graph(nodes*1000, m).to_directed()

nx.write_edgelist(G, "graphSF{}.tsv".format(nodes), delimiter="\t", data=False)
