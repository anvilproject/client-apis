"""Retrieve google bucket meta data, dbGap information and gen3 meta associated with terra workspace."""
import logging

from anvil.terra.workspace import workspace_factory
from anvil.terra.api import get_projects
# from anvil.cache import memoize
from collections import defaultdict
from attrdict import AttrDict

import sqlite3
import pickle


class Reconciler():
    """Retrieve google bucket meta data, dbGap information and gen3 meta associated with terra workspace."""

    def __init__(self, name, user_project, namespaces, project_pattern, avro_path):
        """Initialize properties, set id to namespaces/project_pattern."""
        self.name = name
        self._user_project = user_project
        self.namespaces = namespaces
        self.project_pattern = project_pattern
        self._workspaces = None
        self._logger = logging.getLogger(__name__)
        self.id = f"{namespaces}/{project_pattern}"
        self.avro_path = avro_path

    @property
    def workspaces(self):
        """Terra workspaces that match namespace & project_pattern."""
        if not self._workspaces:
            self._workspaces = [workspace_factory(w, user_project=self._user_project, avro_path=self.avro_path) for w in get_projects(self.namespaces, self.project_pattern)]
            for w in self._workspaces:
                w.attributes['reconciler_name'] = self.name

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

    def save(self):
        """Persist workspaces to db."""
        entities = Entities(path='/tmp/terra.sqlite')
        names = []
        for w in self.workspaces:
            if w.name in names:
                raise Exception(w.name)
            names.append(w.name)
            entities.save(w)
        entities.index()


class Entities:
    """Represent workspace objects."""

    def __init__(self, path):
        """Simplify blob."""
        self.path = path
        self._conn = sqlite3.connect(self.path)
        cur = self._conn.cursor()
        cur.executescript("""
        CREATE TABLE IF NOT EXISTS vertices (
            key text PRIMARY KEY,
            submitter_id text,
            name text,
            json text NOT NULL
        );
        CREATE TABLE IF NOT EXISTS edges (
            src text,
            dst text,
            src_name text,
            dst_name text
        );
        """)
        self._conn.commit()

    def put(self, key, submitter_id, name, data, cur):
        """Save an item."""
        cur.execute("REPLACE into vertices values (?, ?, ?, ?);", (key, submitter_id, name, pickle.dumps(data)))
        # self._logger.debug(f"put {key}")

    def get(self, key=None):
        """Retrieve an item."""
        cur = self._conn.cursor()
        data = cur.execute("SELECT json FROM vertices where key=?", (key,)).fetchone()
        if data:
            v = pickle.loads(data[0])
            src_edges = [(d[0], d[1]) for d in cur.execute("SELECT src, src_name FROM edges where dst=?", (key,)).fetchall()]
            edges = {}
            for src in src_edges:
                if src[1] not in edges:
                    edges[src[1]] = []
                for d in cur.execute("SELECT json, key, name FROM vertices where key=?", (src[0],)).fetchall():                    
                    edges[src[1]].append(pickle.loads(d[0]))

            return {'vertex': v, 'edges': edges}
        assert False, f"NOT FOUND {key}"

    def get_by_name(self, name=None):
        """Retrieve all items with name."""
        cur = self._conn.cursor()
        data = cur.execute("SELECT json FROM vertices where name=?", (name,)).fetchall()
        return [pickle.loads(d[0]) for d in data]

    def put_edge(self, src, dst, src_name, dst_name, cur):
        """Save edge."""
        cur.execute("REPLACE into edges values (?, ?, ?, ?);", (src, dst, src_name, dst_name))

    def save(self, workspace):
        """Load sqlite db from workspace."""
        cur = self._conn.cursor()
        logging.getLogger(__name__).info(f'Loading {workspace.name}')
        self.put(workspace.id, workspace.name, 'workspace', workspace, cur)
        for subject in workspace.subjects:
            self.put(subject.id, subject.id, 'subject', subject, cur)
            self.put_edge(subject.id, workspace.id, 'subject', 'workspace', cur)
            for sample in subject.samples:
                self.put(sample.id, sample.id, 'sample', sample, cur)
                self.put_edge(sample.id, subject.id, 'sample', 'subject', cur)
                for blob_id, blob in sample.blobs.items():
                    self.put(blob_id, blob_id, 'blob', blob, cur)
                    self.put_edge(blob_id, sample.id, 'blob', 'sample', cur)
                    if 'ga4gh_drs_uri' in blob:
                        drs = {'uri': blob['ga4gh_drs_uri']}
                        self.put(drs['uri'], sample.id, 'drs', drs, cur)
                        self.put_edge(drs['uri'], sample.id, 'drs', 'sample', cur)

        self._conn.commit()

    def index(self):
        """Index the vertices and edges."""
        logging.getLogger(__name__).info('Indexing')
        cur = self._conn.cursor()
        cur.executescript("""
        CREATE  INDEX IF NOT EXISTS vertices_submitter_id ON vertices(submitter_id);
        CREATE UNIQUE INDEX IF NOT EXISTS edges_src_dst ON edges(src, dst, src_name, dst_name);
        CREATE  INDEX IF NOT EXISTS edges_dst ON edges(dst);
        """)
        self._conn.commit()
