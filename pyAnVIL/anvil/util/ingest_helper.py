"""Validate AnVIL workspace(s)."""

import os

from google.cloud.storage import Client
from google.cloud.storage.blob import Blob
from collections import defaultdict
import ipywidgets as widgets
from ipywidgets import interact
from IPython.display import display
import pandas as pd
import firecloud.api as FAPI


class IngestHelper():
    """Validate workspace from dropdown selections."""

    def __init__(self, workspace_namespace='terra-test-bwalsh', workspace_name='pyAnVIL Notebook', user_project=os.environ.get('GOOGLE_PROJECT', None)) -> None:
        """Retrieve expected schemas."""
        assert user_project, "AnVIL buckets use the `Requester Pays` feature. Please include a billing project."
        self.WORKSPACES = FAPI.list_workspaces().json()
        self.schemas_table = FAPI.get_entities(workspace_namespace, workspace_name, 'schema').json()
        self.schemas = defaultdict(dict)
        for e in self.schemas_table:
            a = e['attributes']
            self.schemas[a['consortium']][a['entity']] = a
        self.consortiums = widgets.Dropdown(options=['Choose...'] + list(self.schemas.keys()))
        self.workspaces = widgets.Dropdown(options=[])
        self.user_project = user_project
        self.client = Client(project=self.user_project)

    def validate(self, reference_schema, namespace, workspace_name, check_blobs=True):
        """Check target workspace against reference."""
        target_entities = FAPI.list_entity_types(namespace=namespace, workspace=workspace_name).json()
        reference = set(reference_schema.keys())
        target = set(target_entities.keys())
        result = dict(workspace=workspace_name)
        for entity in reference.intersection(target):
            uri = None
            try:
                reference_fields = set([f.replace(' ', '') for f in reference_schema[entity]['required'].split(',')])
                if 'bucket_fields' in reference_schema[entity]:
                    reference_fields.update([f.replace(' ', '') for f in reference_schema[entity].get('bucket_fields', '').split(',')])
                target_fields = set(target_entities[entity]['attributeNames'] + [target_entities[entity]['idName']])
                if not reference_fields.issubset(target_fields):
                    msg = f'fields_missing:{reference_fields - target_fields }'
                else:
                    msg = 'OK'
                result[entity] = msg
                project_buckets = {}
                if 'bucket_fields' in reference_schema[entity]:
                    for bucket_field in reference_schema[entity]['bucket_fields'].split(','):
                        if bucket_field not in target_fields:
                            result[entity] = f"{bucket_field} not found in {entity} schema."
                            continue
                        for e in FAPI.get_entities(namespace, workspace=workspace_name, etype=entity).json():
                            uri = e['attributes'][bucket_field]
                            blob = Blob.from_string(uri, client=self.client)
                            bucket_name = blob.bucket.name
                            if bucket_name not in project_buckets:
                                print(f"checking {workspace_name} {bucket_name}")
                                bucket = self.client.bucket(bucket_name, user_project=self.user_project)
                                project_buckets[bucket_name] = {}
                                for b in list(bucket.list_blobs()):
                                    project_buckets[bucket_name][b.name] = {'size': b.size, 'etag': b.etag, 'crc32c': b.crc32c, 'time_created': b.time_created, 'name': b.name}
                            if blob.name not in project_buckets[bucket_name]:
                                result[entity] = f"{uri} does not exist\n{e}\n{project_buckets[bucket_name].keys()}"
                                break
            except Exception as e:
                print(f"{workspace_name} {uri} {e}")
                result[entity] = str(e)

        for entity in reference - target:
            result[entity] = 'missing'
        result['unknown'] = f"{','.join(sorted(target - reference))}"

        return result

    def interact(self):
        """Use widgets to display drop downs for consortiums and workspaces, handle user selections."""
        pd.set_option("display.max_rows", None, "display.max_columns", None)

        def update_workspaces(*args):
            self.workspaces.options = ['Choose...', 'All workspaces'] + [w['workspace']['name'] for w in self.WORKSPACES if 'anvil-datastorage' in w['workspace']['namespace'] and self.consortiums.value.lower() in w['workspace']['name'].lower()]

        # Tie the image options to directory value
        self.consortiums.observe(update_workspaces, 'value')

        # Show the images
        def show_workspace(consortium, workspace):
            reference_df = pd.DataFrame(self.schemas[consortium]).dropna()
            reference_df = reference_df.style.set_caption("Reference")
            if workspace and workspace == 'All workspaces':
                print("Working...")
                validations = []
                for workspace_name in [w['workspace']['name'] for w in self.WORKSPACES if 'anvil-datastorage' in w['workspace']['namespace'] and self.consortiums.value.lower() in w['workspace']['name'].lower()]:
                    validation = self.validate(self.schemas[consortium], 'anvil-datastorage', workspace_name)
                    validations.append(validation)
                df = pd.DataFrame(validations).set_index('workspace').style.set_caption(f"{consortium}/{workspace}")
                display(reference_df)
                display(df)
                return

            if workspace and workspace != 'Choose...':
                df = pd.DataFrame([self.validate(self.schemas[consortium], 'anvil-datastorage', workspace)])
                df = df.set_index('workspace').style.set_caption(f"{consortium}/{workspace}")
                display(reference_df)
                display(df)
                return

        _ = interact(show_workspace, consortium=self.consortiums, workspace=self.workspaces)
