import networkx as nx
from rdflib import Graph
import matplotlib.pyplot as plt

# Load your .ttl file
g = Graph()
g.parse("museit_topic_graph.ttl", format="ttl")

# Create a NetworkX graph
nx_graph = nx.DiGraph()
for s, p, o in g:
    nx_graph.add_edge(str(s), str(o), label=str(p))

# Visualize the graph
pos = nx.spring_layout(nx_graph)  # Layout algorithm for positioning
nx.draw(nx_graph, pos, with_labels=True, node_size=2000, node_color="lightblue")
nx.draw_networkx_edge_labels(nx_graph, pos, edge_labels={(u, v): d["label"] for u, v, d in nx_graph.edges(data=True)})

plt.title("RDF Graph Visualization")
plt.savefig("museit_topic_graph.png")
