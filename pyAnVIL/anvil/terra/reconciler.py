"""Retrieve google bucket meta data, dbGap information and gen3 meta associated with terra workspace."""
import logging

from anvil.terra.workspace import Workspace
from anvil.terra.api import get_projects
# from anvil.cache import memoize
from collections import defaultdict
from attrdict import AttrDict


class Reconciler():
    """Retrieve google bucket meta data, dbGap information and gen3 meta associated with terra workspace."""

    def __init__(self, name, user_project, namespaces, project_pattern):
        """Initialize properties, set id to namespaces/project_pattern."""
        self.name = name
        self._user_project = user_project
        self.namespaces = namespaces
        self.project_pattern = project_pattern
        self._workspaces = None
        self._logger = logging.getLogger(__name__)
        self.id = f"{namespaces}/{project_pattern}"

    @property
    def workspaces(self):
        """Terra workspaces that match namespace & project_pattern."""
        if not self._workspaces:
            self._workspaces = [Workspace(w, user_project=self._user_project) for w in get_projects(self.namespaces, self.project_pattern)]
        return self._workspaces

    # @memoize
    def reconcile_schemas(self):
        """Report workspaces that share common entities. Sorted in descending order of number of matching workspaces."""
        # sort workspaces into those who share a common schema
        def workspace_len(e):
            return len(e[1])

        workspace_entities = defaultdict(list)
        for w in self.workspaces:
            entity_list = ",".join(sorted(w.schemas.keys()))
            workspace_entities[entity_list].append(w.attributes.workspace.name)

        # print(datetime.datetime.now(), 'start')
        sorted_workspaces = sorted([(entities, workspaces) for (entities, workspaces) in workspace_entities.items()], key=workspace_len, reverse=True)
        reconciled_schemas = {
            'conformant': {
                'entities': sorted_workspaces[0][0].split(","),
                'workspaces': sorted_workspaces[0][1]
            },
            'incompatible': [{'entities': rs[0], 'workspaces': rs[1]} for rs in sorted_workspaces[1:]],
            'schema_conflict_sample': [],
            'schema_conflict_subject': [],
        }
        reconciled_schemas = AttrDict(reconciled_schemas)
        # print(datetime.datetime.now(), 'sorted_schemas')

        # check subjects and samples to see that data conforms to schema
        for name in reconciled_schemas.conformant.workspaces:
            for w in self.workspaces:
                if name == w.attributes.workspace.name:

                    if sorted(w.schemas[w.subject_property_name]['attributeNames']) != sorted(w.subjects[0].attributes.keys()):
                        reconciled_schemas['schema_conflict_subject'].append(name)
                        self._logger.debug(f"{w.name} schema_conflict due to subject.")
                        continue

                    if sorted(w.schemas['sample']['attributeNames']) != sorted(w.samples[0].attributes.keys()):
                        reconciled_schemas['schema_conflict_sample'].append(name)
                        self._logger.debug(f"{w.name} schema_conflict due to sample.")
                        continue

                    if w.subject_property_name not in w.schemas:
                        self._logger.debug(f"ERROR {w.name} no {w.subject_property_name} in schema? {w.schema}.")

        conformant_workspace_names = list(set(reconciled_schemas.conformant.workspaces))

        for name in reconciled_schemas.schema_conflict_sample:
            self._logger.debug(f"removing schema_conflict {name}")
            conformant_workspace_names.remove(name)
        for name in reconciled_schemas.schema_conflict_subject:
            self._logger.debug(f"removing schema_conflict {name}")
            conformant_workspace_names.remove(name)

        # print(datetime.datetime.now(), 'reconciled_schemas')

        return AttrDict(reconciled_schemas)

    @property
    def problems(self):
        """Aggregate all problems."""
        workspaces = self.workspaces
        _problems = {
            'inconsistent_entityName': [w.name for w in workspaces if w.inconsistent_entityName],
            'inconsistent_subject': [w.name for w in workspaces if w.inconsistent_subject],
            'missing_blobs': [w.name for w in workspaces if w.missing_blobs],
            'missing_samples': [w.name for w in workspaces if w.missing_samples],
            'missing_project_files': [w.name for w in workspaces if w.missing_project_files],
            'missing_subjects': [w.name for w in workspaces if w.missing_subjects],
        }
        return [k for k, v in _problems.items() if v]

    @property
    def blob_sizes(self):
        """Aggregate blob sizes by property name."""
        _blob_sizes = defaultdict(int)
        for w in self.workspaces:
            for k, v in w.blob_sizes.items():
                _blob_sizes[k] += v
        return _blob_sizes

    @property
    def project_file_blob_sizes(self):
        """Aggregate blob sizes by unique blob name."""
        _blob_sizes = {}
        for w in self.workspaces:
            # print(w.name)
            if w.project_files:
                for k, v in w.project_files.items():
                    # print(w.name)
                    # print(k, v)
                    _blob_sizes[v.value] = v.blob.size
        return _blob_sizes

    @property
    def dashboard_views(self):
        """Aggregate dashboard views."""
        for w in self.workspaces:
            v = w.dashboard_view
            v['problems'] = [k for k, v in v['problems'].items() if v]
            v['source'] = self.name
            #     "source": "CCDG",
            #     "gen3_project_id": null,
            #     "gen3_file_histogram": null,
            #     "dbGAP_project_id": null,
            #     "dbGAP_study_id": "phs001155",
            #     "dbGAP_acession": null,
            #     "dbGAP_sample_count": null
            # }

            yield v
