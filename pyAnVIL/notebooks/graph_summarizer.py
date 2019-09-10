from IPython.display import Image
import pygraphviz
from collections import defaultdict


def summarize_graph(graph):
    """Introspects the data in the graph, creates a summary graph using pygraphviz.  Relies on label attribute on each node"""
    # calc labels and edge lables
    labels = defaultdict(int)
    for k, v in graph.nodes.data():
        labels[v['label']] += 1
    for k, v in labels.items():
        labels[k] = '{}({})'.format(k, v)

    edge_labels = defaultdict(int)
    for n in graph.nodes():
        lable = graph.node[n]['label']
        for neighbor in graph.neighbors(n):
            n_lable = graph.node[neighbor]['label']
            edges = graph.get_edge_data(n, neighbor)
            for e in edges.values():
                edge_labels[(lable, n_lable, e['label'])] += 1
    for k, v in edge_labels.items():
        edge_labels[k] = '{}'.format(v)

    # create new summary graph
    g = pygraphviz.AGraph(strict=False, directed=True)

    for k in labels:
        g.add_node(k, label=labels[k])

    compass_points = [('e', 'w')]

    for k in edge_labels:
        start = k[0]
        end = k[1]
        # key = k[2]
        # use compass points for self loops
        opts = {}
        if start == end:
            compass_point_offset = len([e for e in g.out_edges([start]) if e[1] == start]) % len(compass_points)
            compass_point = compass_points[compass_point_offset]
            opts = {'headport': compass_point[1], 'tailport': compass_point[0]}
        g.add_edge(start, end, label='{}({})'.format(k[2], edge_labels[k]), labeldistance=0, **opts)

    return g


def draw_summary(g, label='<untitled>', prog='dot'):
    # ['dot', 'neato', 'twopi', 'circo', 'fdp', 'sfdp']
    g.layout(prog)
    g.graph_attr.update(label=label, size='40,40', pad=1)
    g.edge_attr.update(arrowsize='0.6', style='dotted')
    g.graph_attr.update(scale=3)  # , nodesep=1, ratio='auto')
    g.write(f'notebooks/figures/{label}.dot')
    g.draw('notebooks/figures/{}.png'.format(label))
    return Image('notebooks/figures/{}.png'.format(label))
