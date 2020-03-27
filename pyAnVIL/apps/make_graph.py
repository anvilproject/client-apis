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
from anvil import install_cache
from anvil.transformers.eMERGE import eMERGE

from apps.graph_summarizer import summarize_graph, draw_summary, draw_samples_attributes, draw_workspace_attributes
from apps.node_counts import create_table

import json


def make_transformers(program, user_project):
    return [
        CCDG(program=program, user_project=user_project),
        CMG(program=program, user_project=user_project),
        GTEx(program=program, user_project=user_project),
        ThousandGenomes(program=program, user_project=user_project),
        eMERGE(program=program, user_project=user_project)
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


def generate_project_summary(transformers):
    """Returns array of tuples (transformer_name, graph, counts)."""
    for t in transformers:
        name = t.__class__.__name__
        project_summary = t.graph_project_summary()
        for project_id, summary in project_summary.items():
            # strip off program prefix
            summary['project_id'] = project_id.split('/')[-1]
            summary['source'] = name
            summary['gen3_project_id'] = None
            summary['gen3_file_histogram'] = None
            summary['dbGAP_project_id'] = None
        yield project_summary


@click.command()
@click.option('--namespace', default='anvil-datastorage', help='Terra namespace to query')
@click.option('--user_project', envvar='USER_PROJECT', help='Google billing project for requestor pays')
@click.option('--report', default=False, help='Generate local "uber graph" report.')
def main(namespace, user_project, report):
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

    with open(f"node_counts.json", 'w') as fd:
        json.dump(node_counts, fd)

    # compose into uber graph
    if report:
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
        # https://app.zenhub.com/workspaces/anvil-portal-5cccd4e3d9d2da0571fb3427/issues/anvilproject/anvil-portal/155#issuecomment-564664519
        project_summaries = []
        for project_summary in generate_project_summary(transformers):
            for p in project_summary.values():
                # turn dicts into arrays
                p["files"] = [v for n, v in p["files"].items()]
                p["nodes"] = [v for n, v in p["nodes"].items()]
            project_summaries.extend(project_summary.values())
        projects = {'projects': project_summaries}
        json.dump(projects, output, separators=(',', ': '))


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())
    install_cache()
    main()
