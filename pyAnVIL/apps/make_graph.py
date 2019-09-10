import logging

import click
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
import networkx as nx

from anvil.transformers.ccdg import CCDG
from anvil.transformers.cmg import CMG
from anvil.transformers.gtex import GTEx
from anvil.transformers.thousand_genomes import ThousandGenomes
from apps.graph_summarizer import summarize_graph, draw_summary


def generate_graphs():
    """Returns array of tuples (transformer_name, graph)."""
    transformers = [CCDG(), CMG(), GTEx(), ThousandGenomes()]
    for t in transformers:
        yield (t.__class__.__name__, t.to_graph(), t.graph_node_counts())


@click.command()
def main():
    """Harvests and transforms terra data to graph."""
    logger = logging.getLogger(__name__)
    logger.info(f'Node counts:')
    graphs = []
    node_counts = {}
    for name, graph, counts in generate_graphs():
        logger.info(f'{name}: {len(graph.nodes())}')
        draw_summary(summarize_graph(graph), f'{name} participants, samples, and files', prog='dot')
        graphs.append(graph)
        node_counts.update(counts)
    # compose into uber graph
    anvil = nx.compose_all(graphs)
    logger.info(f'AnVIL: {len(anvil.nodes())}')
    draw_summary(summarize_graph(anvil), f'AnVIL participants, samples, and files', save_dot_file=True)


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
