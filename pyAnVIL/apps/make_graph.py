import logging
from datetime import date

import click
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
import networkx as nx

from anvil.transformers.ccdg import CCDG
from anvil.transformers.cmg import CMG
from anvil.transformers.gtex import GTEx
from anvil.transformers.thousand_genomes import ThousandGenomes
# from anvil.transformers.eMERGE import eMERGE

from apps.graph_summarizer import summarize_graph, draw_summary, draw_samples_attributes, draw_workspace_attributes
from apps.node_counts import create_table

import json


def make_transformers(program, user_project):
    return [
        CCDG(program=program, user_project=user_project),
        CMG(program=program, user_project=user_project),
        GTEx(program=program, user_project=user_project),
        ThousandGenomes(program=program, user_project=user_project),
        # eMERGE(program=program, user_project=user_project)
    ]


def generate_graphs(transformers):
    """Returns array of tuples (transformer_name, graph, counts)."""
    for t in transformers:
        name = t.__class__.__name__
        graph = t.to_graph()
        counts = t.graph_node_counts()
        flattened = []
        for project_id, count in counts.items():
            # strip off program prefix
            count['project_id'] = project_id.split('/')[-1]
            count['source'] = name
            flattened.append(count)
        yield (name, graph, flattened)

    return transformers


@click.command()
@click.option('--namespace', default='anvil-datastorage', help='Terra namespace to query')
@click.option('--user_project', envvar='USER_PROJECT', help='Google billing project for requestor pays')
def main(namespace, user_project):
    """Harvests and transforms terra data to graph."""
    logger = logging.getLogger(__name__)
    logger.info(f'Node counts:')
    graphs = []
    node_counts = []

    transformers = make_transformers(namespace, user_project)
    for name, graph, counts in generate_graphs(transformers):
        logger.info(f'{name}: {len(graph.nodes())}')
        draw_summary(summarize_graph(graph), f'{name} participants, samples, and files', prog='dot')
        graphs.append(graph)
        node_counts.extend(counts)
    # compose into uber graph
    anvil = nx.compose_all(graphs)
    logger.info(f'AnVIL: {len(anvil.nodes())}')
    draw_summary(summarize_graph(anvil), f'AnVIL participants, samples, and files', save_dot_file=True, scale=6)
    table = create_table(node_counts)
    draw_samples_attributes(transformers)
    draw_workspace_attributes(transformers)
    with open('apps/report.md.template') as input:
        report = input.read()
    report = report.format(table=table, date_generated=date.today())
    with open('notebooks/figures/report.md', 'w') as output:
        output.write(report)
    with open('notebooks/figures/report-data.json', 'w') as output:
        for node_count in node_counts:
            json.dump(node_count, output)
            output.write("\n")


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.WARNING, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
