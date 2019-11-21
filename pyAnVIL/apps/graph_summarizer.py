from IPython.display import Image
import pygraphviz
from collections import defaultdict
import humanfriendly

import matplotlib.pyplot
import pandas
import upsetplot

import firecloud.api as FAPI
from attrdict import AttrDict


def summarize_graph(graph):
    """Introspects the data in the graph, creates a summary graph.  Relies on label attribute on each node"""
    # calc labels and edge lables
    labels = defaultdict(int)
    sizes = defaultdict(int)
    for k, v in graph.nodes.data():
        labels[v['label']] += 1
        if 'size' in v:
            sizes[v['label']] += v['size']
    for k, v in labels.items():
        labels[k] = '{}({})'.format(k, v)

    for k, v in labels.items():
        if k in sizes:
            labels[k] = '{}({})'.format(v, humanfriendly.format_size(sizes[k]))

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
        g.add_edge(start, end, label=f'{k[2]}({edge_labels[k]})', labeldistance=0, **opts)

    return g


def draw_summary(g, label='<untitled>', prog='dot', name=None, save_dot_file=False, size='40,40', scale=3):
    """Creates network graph figure using pygraphviz."""
    # ['dot', 'neato', 'twopi', 'circo', 'fdp', 'sfdp']
    g.layout(prog)
    g.graph_attr.update(label=label, size=size, pad=1)
    g.edge_attr.update(arrowsize='0.6', style='dotted')
    g.graph_attr.update(scale=scale)  # , nodesep=1, ratio='auto')
    # if not set, default to first word in label
    if not name:
        name = label.split()[0]
    if save_dot_file:
        g.write(f'notebooks/figures/{name}.dot')
    g.draw(f'notebooks/figures/{name}.png')
    return Image(f'notebooks/figures/{name}.png')


def draw_samples_attributes(transformers):
    """Upset plots for sample attributes."""

    samples = []
    for t in transformers:
        for s in t.get_terra_samples():
            samples.append(s)

    sample_df = pandas.DataFrame(upsetplot.from_contents({s.project_id: s.keys() for s in samples}))

    upsetplot.plot(sample_df, sort_by="cardinality", sum_over=False, show_counts='%d')
    current_figure = matplotlib.pyplot.gcf()
    current_figure.suptitle('Count of shared sample properties')
    current_figure.savefig("notebooks/figures/sample_projects.png")

    entity_by_project = defaultdict(set)

    for s in samples:
        for k in s.keys():
            entity_by_project[k].add(s.project_id)

    entity_df = pandas.DataFrame(upsetplot.from_contents(entity_by_project))
    upsetplot.plot(entity_df, sort_by="cardinality", sum_over=False, show_counts='%d')
    current_figure = matplotlib.pyplot.gcf()
    current_figure.set_size_inches(10.5, 40.5)
    current_figure.suptitle('"Sample" Count of shared attribute names')
    current_figure.savefig("notebooks/figures/sample_attributes.png")


def draw_workspace_attributes(transformers):
    """Upset plots for workspace attributes."""
    workspaces = FAPI.list_workspaces().json()

    workspace_attributes = []
    for w in workspaces:
        workspace_attributes.append(AttrDict({'name': w['workspace']['name'], 'attribute_keys': sorted(list(w['workspace']['attributes'].keys()))}))

    workspace_df = pandas.DataFrame(upsetplot.from_contents({w.name: w.attribute_keys for w in workspace_attributes}))

    current_figure = matplotlib.pyplot.gcf()
    current_figure.set_size_inches(10.5, 80.5)
    upsetplot.plot(workspace_df, sort_by="cardinality", sum_over=False, show_counts='%d')
    current_figure.suptitle('Count of shared workspace properties')
    current_figure.savefig("notebooks/figures/workspace_projects.png")

    entity_by_project = defaultdict(set)

    for w in workspace_attributes:
        for k in w.attribute_keys:
            entity_by_project[k].add(w.name)

    entity_df = pandas.DataFrame(upsetplot.from_contents(entity_by_project))
    current_figure = matplotlib.pyplot.gcf()
    current_figure.set_size_inches(10.5, 80.5)
    upsetplot.plot(entity_df, sort_by="cardinality", sum_over=False, show_counts='%d')
    current_figure.suptitle('"Workspace" Count of shared attribute names')
    current_figure.savefig("notebooks/figures/workspace_attributes.png")
