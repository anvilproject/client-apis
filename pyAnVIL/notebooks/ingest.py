import firecloud.api as FAPI
import pandas as pd
from anvil.util.reconciler import flatten
import os
import json
import datetime
import logging

import ipywidgets as widgets
from IPython.display import display
from IPython.display import clear_output
from ipywidgets import AppLayout, GridspecLayout
from ipywidgets import interact

import pandas as pd
from upsetjs_jupyter_widget import UpSetJSWidget
import matplotlib.pyplot as plt
from pprint import pprint
import json

from anvil.terra.api import get_projects, get_entities, get_schema
from collections import defaultdict


from IPython.core.display import display, HTML
display(HTML("<style>.container { width:100% !important; }</style>"))
display(HTML("<style>.output_result { max-width:100% !important; }</style>"))
display(HTML("<style>.prompt { display:none !important; }</style>"))


logging.basicConfig(level=logging.ERROR, format='%(asctime)s %(levelname)-8s %(message)s')
DASHBOARD_OUTPUT_PATH = "/tmp"
TERRA_SUMMARY = f"{DASHBOARD_OUTPUT_PATH}/terra_summary.json"
DASHBOARD_OUTPUT_FILE = f"{DASHBOARD_OUTPUT_PATH}/data_dashboard.json"

PROBLEMS = """
dbgap_sample_count_mismatch
inconsistent_entityName
inconsistent_subject
missing_accession
missing_blobs
missing_samples
missing_schema
missing_sequence
missing_subjects
schema_incompatible
schema_conflict_sample
schema_conflict_subject
""".split()


def unique_sorted_values(array):
    """Unique list."""
    unique = array.unique().tolist()
    unique.sort()
    return unique


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def get_dashboard_data():
    """Fetch the dashboard data."""
    assert os.path.isfile(DASHBOARD_OUTPUT_FILE), "dashboard should exist"
    with open(DASHBOARD_OUTPUT_FILE, 'r') as inputs:
        dashboard_data = json.load(inputs)
        # dashboard_data = schema_problems(dashboard_data)
        return dashboard_data


def schema_problems(dashboard_data, show_compliant):
    """Add schema problems."""

    def workspace_len(e):
        return len(e[1])

    consortiums = {}
    # get all the workspaces and the expected entities
    for consortium in dashboard_data['consortiums']:
        consortiums[consortium['name']] = {}
        consortiums[consortium['name']]['expected_entities'] = consortium['conformant']['entities']
        consortiums[consortium['name']]['workspaces'] = consortium['conformant']['workspaces']
        for problem in ['incompatible']:
            for schema_variation in consortium[problem]:
                consortiums[consortium['name']]['workspaces'].extend(schema_variation['workspaces'])
        for problem in ['schema_conflict_sample', 'schema_conflict_subject']:
            consortiums[consortium['name']]['workspaces'].extend(consortium[problem])
        consortiums[consortium['name']]['workspaces'] = list(set(consortiums[consortium['name']]['workspaces']))

    # determine workspaces that share entity attributes
    for consortium_name in consortiums:
        for workspace in consortiums[consortium_name]['workspaces']:
            schema = get_schema('anvil-datastorage', workspace)
            for entity in consortiums[consortium_name]['expected_entities']:
                if entity not in schema:
                    entity_schema = f'{entity}_missing'
                else:
                    entity_schema = ','.join(sorted(schema[entity]['attributeNames']))
                if 'entity_schemas' not in consortiums[consortium_name]:
                    consortiums[consortium_name]['entity_schemas'] = {}
                if entity not in consortiums[consortium_name]['entity_schemas']:
                    consortiums[consortium_name]['entity_schemas'][entity] = {}
                if entity_schema not in consortiums[consortium_name]['entity_schemas'][entity]:
                    consortiums[consortium_name]['entity_schemas'][entity][entity_schema] = []
                consortiums[consortium_name]['entity_schemas'][entity][entity_schema].append(workspace)

    # mark the workspace
    for p in dashboard_data['projects']:
        p['problems'] = []
    for consortium_name in consortiums:
        for entity in consortiums[consortium_name]['entity_schemas']:
            sorted_workspaces = sorted([(entity_schema, workspaces) for (entity_schema, workspaces) in consortiums[consortium_name]['entity_schemas'][entity].items()], key=workspace_len, reverse=True)
            compliant_marked = False
            for sorted_workspace in sorted_workspaces:
                if not compliant_marked:
                    for w in sorted_workspace[1]:
                        for p in dashboard_data['projects']:
                            if p['project_id'] == w and show_compliant:
                                p['problems'].append(f'schema_compliant_{entity}')
                    compliant_marked = True
                    continue
                if 'missing' in sorted_workspace[0]:
                    for w in sorted_workspace[1]:
                        for p in dashboard_data['projects']:
                            if p['project_id'] == w and not show_compliant:
                                p['problems'].append(sorted_workspace[0])
                    continue
                for w in sorted_workspace[1]:
                    for p in dashboard_data['projects']:
                        if p['project_id'] == w and not show_compliant:
                            p['problems'].append(f'{entity}_not_compliant')
    return dashboard_data


# def schema_problems(workspace, dashboard_data):
#     """Add schema problems."""
#     _problems = []
#     for consortium in dashboard_data['consortiums']:
#         for problem in ['incompatible', 'schema_conflict_sample', 'schema_conflict_subject', ]:
#             workspaces_with_problem = []
#             if isinstance(consortium[problem], list):
#                 for e in consortium[problem]:
#                     if isinstance(e, str):
#                         workspaces_with_problem.append(e)
#                     else:
#                         for w in e['workspaces']:
#                             workspaces_with_problem.append(w)
#             else:
#                 workspaces_with_problem = consortium[problem]['workspaces']
#             if workspace in workspaces_with_problem:
#                 if 'schema' not in problem:
#                     problem = f"schema_{problem}"
#                 _problems.append(problem)
#                 break
#     return _problems


def flattened_dataframe(dashboard_data):
    """Flatten the data into a tsv."""
    # Flatten dashboard into tsv
    (flattened, column_names) = flatten(dashboard_data['projects'])
    df = pd.DataFrame(flattened)
    df.columns = column_names
    # Print the data  (all rows, all columns)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    # export create a tsv from dataframe
    return df


class IngestHelper(object):
    """Encapsulate dataframe."""

    def __init__(self, dashboard_data=None) -> None:
        """Fetch and flatten."""
        super().__init__()
        if not dashboard_data:
            dashboard_data = get_dashboard_data()
        self.dashboard_data = dashboard_data
        self.df = flattened_dataframe(dashboard_data)
        self.dropdown_workspace = None
        self.dropdown_problems = None
        self.rendered_df = None
        self.problems = PROBLEMS
        self.workspace = ''
        self.problem = ''

    def workspace_eventhandler(self, change):
        """Render choice."""
        clear_output(wait=True)
        self.rendered_df = self.df.query(f'workspace == "{change.new}"').T
        self.render()

    def problems_eventhandler(self, change):
        """Render choice."""
        clear_output(wait=True)
        try:
            self.rendered_df = self.df.query(f'{change.new} == True').T
        except Exception as e:
            print(f"{change.new} {e}")
        self.render()

    def widgets(self):
        """Render a set of dropdowns."""
        self.dropdown_workspace = widgets.Dropdown(options=unique_sorted_values(self.df.workspace))
        self.dropdown_workspace.observe(self.workspace_eventhandler, names='value')

        self.dropdown_problems = widgets.Dropdown(options=self.problems)
        self.dropdown_problems.observe(self.problems_eventhandler, names='value')

        header = GridspecLayout(1, 2)
        header[0, 0] = self.dropdown_workspace
        header[0, 1] = self.dropdown_problems

        center = None
        if self.rendered_df is not None:
            center = GridspecLayout(1, 1)
            center[0, 0] = self.rendered_df

        return AppLayout(
            header=header,
            left_sidebar=None,
            center=center,
            right_sidebar=None,
            footer=None,
            pane_widths=[0, 1, 0],
        )

    def render(self):
        display(self.widgets())

    def create_upset(self):
        flattened = {}
        problems = set([problem for project in self.dashboard_data['projects'] for problem in project['problems']])
        for problem in problems:
            projects = [project['project_id'] for project in self.dashboard_data['projects'] if problem in project['problems'] and project['project_id'] != 'dbGAP']
            if len(projects):
                flattened[problem] = projects

        w = UpSetJSWidget[str](bar_label=1)
        w.mode = 'click'
        w.from_dict(flattened)
        return w

    def show_set(self, _set):
        """Display set."""
        df = pd.DataFrame(list(_set.elems))
        df.columns = [_set.name]
        # Print the data  (all rows, all columns)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        return df

    def upset_selection_changed(self, selection):
        """Callback from widget. """
        if selection:
            return self.show_set(selection)
        return None


h = IngestHelper()


def create_upset(dashboard_data):
    flattened = {}
    problems = set([problem for project in dashboard_data['projects'] for problem in project['problems']])
    for problem in problems:
        projects = [project['project_id'] for project in dashboard_data['projects'] if problem in project['problems'] and project['project_id'] != 'dbGAP']
        if len(projects):
            flattened[problem] = projects

    w = UpSetJSWidget[str](bar_label=1)
    w.mode = 'click'
    w.from_dict(flattened)
    return w


def show_set(_set):
    """Display set."""
    workspace_select = widgets.Select(
        options=list(_set.elems),
        rows=10,
        description=_set.name,
        disabled=False
    )
    workspace_select.layout.width = '40em'
    interact(workspace_changed, workspace=workspace_select)


def upset_selection_changed(workspace_issues):
    """Callback from widget. """
    if workspace_issues:
        return show_set(workspace_issues)
    return None

    # @property
    # def institute(self):
    #     """Deduce institute."""
    #     _institute = self.attributes.workspace.attributes.get("library:institute", None)
    #     if _institute and 'items' in _institute:
    #         return _institute['items'][0]
    #     return _institute

    # @property
    # def diseaseOntologyId(self):
    #     """Deduce disease."""
    #     _diseaseOntologyID = self.attributes.workspace.attributes.get('diseaseOntologyID', None)
    #     if not _diseaseOntologyID:
    #         _diseaseOntologyID = self.attributes.workspace.attributes.get('library:diseaseOntologyID', None)
    #     if _diseaseOntologyID:
    #         _diseaseOntologyID = _diseaseOntologyID.split('/')[-1].replace('_', ':')
    #     else:
    #         logging.debug(f"{self.id} missing diseaseOntologyID")
    #     return _diseaseOntologyID


def workspace_changed(workspace):
    """Callback from widget."""
    if workspace:

        df = h.df.query(f'workspace == "{workspace}"')
        df = pd.DataFrame(data=df)
        entity_types = FAPI.list_entity_types('anvil-datastorage', workspace).json()
        for e in entity_types.keys():
            df[f'terra_entity_{e}'] = json.dumps(entity_types[e]['attributeNames'], separators=(',', ':'))
        # df['terra_get_entities'] = json.dumps(, separators=(',', ':'))
        # df['terra_list_entity_types'] = json.dumps(FAPI.list_entity_types('anvil-datastorage', workspace).json(), separators=(',', ':'))

        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_colwidth', None)
        pd.set_option('display.colheader_justify', 'left')

        df = df.T
        return df.style.set_properties(**{'text-align': 'left'})
    return None


def show_problems_upset():
    interact(upset_selection_changed, workspace_issues=create_upset(get_dashboard_data()))
    plt.show()


def show_schema_upset(source, show_compliant=False):
    dashboard_data = get_dashboard_data()
    keys_to_delete = []
    for project in dashboard_data['projects']:
        if project['source'] != source:
            keys_to_delete.append(project['project_id'])
    dashboard_data['projects'] = [project for project in dashboard_data['projects'] if project['project_id'] not in keys_to_delete]
    interact(upset_selection_changed, workspace_issues=create_upset(schema_problems(dashboard_data, show_compliant)))
    plt.show()


def show_project_schema_compliance(dashboard_data, consortium_name):
    consortium = [c for c in dashboard_data['consortiums'] if c['name'] == consortium_name][0]
    entitites = sorted(consortium['conformant']['entities'])

    def _data():
        for workspace in dashboard_data['projects']:
            if workspace['source'] != consortium_name:
                continue
            yield [workspace['source'], workspace['project_id']] + [(f"schema_compliant_{e}" in workspace['problems']) for e in entitites]

    consensus_schema = consortium['consensus_schema']
    df = pd.DataFrame([(e, ','.join(consensus_schema[e]['attributeNames'])) for e in consensus_schema], columns=['entity', 'columns'])
    df = df.set_index(['entity'])
    df = df.sort_values(by='entity')
    display(df.style.set_properties(**{'text-align': 'left'}))

    df = pd.DataFrame(_data(), columns=['consortium', 'name'] + entitites)
    df = df.set_index(['consortium', 'name'])
    df = df.sort_values(by=entitites, ascending=False)
    display(df)
