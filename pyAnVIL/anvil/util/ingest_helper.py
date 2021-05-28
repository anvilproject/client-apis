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
from types import SimpleNamespace
import numpy as np


class NestedNamespace(SimpleNamespace):
    """Extend SimpleNamespace."""

    def __init__(self, dictionary, **kwargs):
        """Initialize nested attributes."""
        super().__init__(**kwargs)
        for key, value in dictionary.items():
            if isinstance(value, dict):
                self.__setattr__(key, NestedNamespace(value))
            else:
                self.__setattr__(key, value)


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
        self.reference_schema = None

    def validate(self, reference_schema, namespace, workspace_name, check_blobs=True):
        """Check target workspace against reference."""
        target_entities = FAPI.list_entity_types(namespace=namespace, workspace=workspace_name).json()
        reference = set(reference_schema.keys())
        reference.remove('attributes')
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
                                result[entity] = f"{uri} does not exist in project_buckets {bucket_name}"
                                break
            except Exception as e:
                print(f"{workspace_name} {uri} {e}")
                result[entity] = str(e)

        for entity in reference - target:
            if entity == 'linked_field':
                continue
            result[entity] = 'missing'
        result['unknown_entities'] = f"{','.join(sorted(target - reference))}"
        required_attributes = set([k.replace(' ', '') for k in reference_schema['attributes']['required'].split(',')])
        workspace_attribute_values = FAPI.get_workspace(namespace, workspace_name).json()['workspace']['attributes']
        target_attributes = set(list(workspace_attribute_values.keys()) + [f"library:{e}" for e in workspace_attribute_values.get('library', {}).keys()])

        missing_workspace_keys = sorted(list(required_attributes - target_attributes))
        if len(missing_workspace_keys) == 0:
            result['missing_workspace_keys'] = 'OK'
        else:
            result['missing_workspace_keys'] = ','.join(missing_workspace_keys)

        missing_xrefs = self.cross_ref(reference_schema, namespace, workspace_name)
        result['missing_xrefs'] = ','.join(missing_xrefs)
        return result

    def cross_ref(self, reference_schema, namespace, workspace_name):
        """Evaluate 'join' between two entities."""
        if 'linked_field' not in reference_schema:
            return []

        def get_property(entity, entity_name, expression):
            return eval(expression, {entity_name: NestedNamespace(entity)})

        item = reference_schema['linked_field']
        join = item['relationship']
        (left, right) = join.split('=')
        # print(left, right)
        left_entity = left.split('.')[0]
        right_entity = right.split('.')[0]
        left_keys = set([get_property(e, left_entity, left) for e in FAPI.get_entities(namespace, workspace=workspace_name, etype=left_entity).json()])
        right_keys = set([get_property(e, right_entity, right) for e in FAPI.get_entities(namespace, workspace=workspace_name, etype=right_entity).json()])
        return left_keys - right_keys

    def interact(self):
        """Use widgets to display drop downs for consortiums and workspaces, handle user selections."""
        pd.set_option("display.max_rows", None, "display.max_columns", None)

        def update_workspaces(*args):
            self.workspaces.options = ['Choose...', 'All workspaces', 'This workspace'] + sorted([w['workspace']['name'] for w in self.WORKSPACES if 'anvil-datastorage' in w['workspace']['namespace'] and self.consortiums.value.lower() in w['workspace']['name'].lower()])

        # Tie the image options to directory value
        self.consortiums.observe(update_workspaces, 'value')

        # Show the images
        def show_workspace(consortium, workspace):
            namespace = 'anvil-datastorage'
            self.reference_schema = self.schemas[consortium]
            reference_df = pd.DataFrame(
                [dict(id=e['name'], **e['attributes']) for e in self.schemas_table]
            ).set_index('id').query(f'consortium == "{consortium}"').replace(np.nan, '', regex=True).style.set_caption("Reference")

            if workspace and workspace == 'All workspaces':
                print("Working...")
                validations = []
                for workspace_name in [w['workspace']['name'] for w in self.WORKSPACES if 'anvil-datastorage' in w['workspace']['namespace'] and self.consortiums.value.lower() in w['workspace']['name'].lower()]:
                    validation = self.validate(self.schemas[consortium], namespace, workspace_name)
                    validations.append(validation)
                df = pd.DataFrame(validations).set_index('workspace').style.set_caption(f"{consortium}/{workspace}")
                display(reference_df)
                display(df)
                return
            if workspace == 'This workspace':
                workspace = os.environ['WORKSPACE_NAME']
                namespace = os.environ['WORKSPACE_NAMESPACE']

            if workspace and workspace != 'Choose...':
                df = pd.DataFrame([self.validate(self.schemas[consortium], namespace, workspace)])
                df = df.set_index('workspace').style.set_caption(f"{consortium}/{workspace}")
                display(reference_df)
                display(df)
                return

        _ = interact(show_workspace, consortium=self.consortiums, workspace=self.workspaces)
