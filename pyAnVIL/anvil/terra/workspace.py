"""Client for reconciling terra workspace with the Google Cloud Storage API, dbGap and Gen3."""

import logging
from attrdict import AttrDict
from google.cloud import storage
from collections import defaultdict
from anvil.util.cache import memoize  # cache
from urllib.parse import urlparse
from datetime import datetime

from anvil.terra.api import get_entities, get_schema
from anvil.terra.subject import subject_factory
from anvil.terra.sample import sample_factory


class Workspace():
    """Represent terra workspace."""

    def __init__(self, *args, user_project=None, avro_path=None):
        """Pass all args to AttrDict, set id for cacheing."""
        self.attributes = AttrDict(*args)
        assert user_project, "Must have user_project"
        self._user_project = user_project
        self._logger = logging.getLogger(__name__)
        self.id = self.attributes.workspace.name
        self._subjects = None
        self._schemas = None
        self._samples = None
        self._blobs = None
        self._project_files = None
        self._missing_project_files = None
        self.missing_sequence = False
        self.avro_path = avro_path

    @property
    def subjects(self):
        """Return raw subjects from terra."""
        if not self._subjects:
            self._subjects = [subject_factory(s, workspace=self, samples=self.samples) for s in self._get_entities(self.subject_property_name)]
        return self._subjects

    @property
    def samples(self):
        """Return raw samples from terra indexed by subject_id."""
        if not self._samples:
            self._samples = defaultdict(list)
            blobs = self.blobs()
            sequencing = self._get_entities('sequencing')
            for s in self._get_entities('sample'):
                s = sample_factory(s, workspace=self, blobs=blobs, sequencing=sequencing, avro_path=self.avro_path)
                self._samples[s.subject_id].append(s)
                if s.missing_sequence:
                    self.missing_sequence = s.missing_sequence
        return self._samples

    @property
    def project_files(self):
        """Return files associated with project attributes."""
        if not self._project_files:
            self._project_file_blobs()
        return self._project_files

    @property
    def project_files_attributes(self):
        """Find attributes that are files."""
        _files = {}
        for k, v in self.attributes.workspace.items():
            if isinstance(v, str) and v.startswith('gs://'):
                _files[k] = v
        return _files

    @property
    def missing_project_files(self):
        """Return files associated with project attributes where gs:// blobs are missing."""
        if not self._missing_project_files:
            self._project_file_blobs()
        return self._missing_project_files

    def _project_file_blobs(self):
        """Retrieve all blobs in terra bucket associated with workspace, dict keyed by object url."""
        project_files = _project_files(self.attributes.workspace)
        project_files_keys = project_files.keys()
        if len(project_files_keys) > 0:
            project_buckets = set([urlparse(f).netloc for f in project_files.values()])
            project_blobs = {}
            for project_bucket in project_buckets:
                project_blobs = {**project_blobs, **_bucket_contents(self._user_project, project_bucket)}
            project_blobs = AttrDict(project_blobs)
            for k, v in project_files.items():
                b = project_blobs.get(v, None)
                if not b:
                    if not self._missing_project_files:
                        self._missing_project_files = AttrDict({})
                    self._missing_project_files[k] = {'value': v, 'blob': None}
                else:
                    project_files[k] = AttrDict({'value': v, 'blob': b})
            if self._missing_project_files:
                for k in self._missing_project_files:
                    del project_files[k]
            self._project_files = AttrDict(project_files)

    @memoize
    def blobs(self):
        """Retrieve all blobs in terra bucket associated with workspace, dict keyed by object url.

        Checks workspace.bucketName and workspace.project_files
        :type project: str or None
        :param project: the project which the client acts on behalf of. Will be
                        passed when creating a topic.  If not passed,
                        falls back to the default inferred from the environment.
        """
        if not self._blobs:
            workspace = self.attributes.workspace
            # Instantiates a google client, & get all blobs in bucket
            storage_client = storage.Client(project=self._user_project)
            bucket = storage_client.bucket(workspace['bucketName'], user_project=self._user_project)
            # get subset of data
            _blobs = {}
            try:
                for b in bucket.list_blobs(fields='items(size, etag, crc32c, name, timeCreated),nextPageToken'):
                    name = f"gs://{workspace['bucketName']}/{b.name}"
                    # cache.put(name, {'size': b.size, 'etag': b.etag, 'crc32c': b.crc32c, 'time_created': b.time_created, 'name': name})
                    _blobs[name] = AttrDict({'size': b.size, 'etag': b.etag, 'crc32c': b.crc32c, 'time_created': b.time_created, 'name': name})
                self._blobs = _blobs
            except Exception as e:
                print(f"{self.id} {workspace['bucketName']} {e}")
                self._blobs = _blobs
        return self._blobs

    @property
    def missing_samples(self):
        """Test if any missing samples."""
        missing = [s for s in self.subjects if len(s.samples) == 0]
        if len(missing) == 0:
            return None
        return missing

    @property
    def missing_subjects(self):
        """Test if any missing subjects."""
        return len(self.subjects) == 0

    @property
    def missing_blobs(self):
        """Test if any sample missing blobs."""
        missing = [s for s in self.subjects if len([sa for sa in s.samples if sa.missing_blobs]) > 0]
        if len(missing) == 0:
            return None
        return missing

    @property
    def inconsistent_entityName(self):
        """Test if any samples with inconsistent_entityName."""
        a = [s for s in self.subjects if len([sa for sa in s.samples if sa.inconsistent_entityName]) > 0]
        if len(a) == 0:
            return None
        return a

    @property
    def inconsistent_subject(self):
        """Test if any samples with inconsistent_entityName."""
        a = [s for s in self.subjects if len([sa for sa in s.samples if sa.inconsistent_subject]) > 0]
        if len(a) == 0:
            return None
        return a

    @property
    def blob_sizes(self):
        """Aggregate sample blob sizes by property name."""
        _blob_sizes = defaultdict(int)
        for s in self.subjects:
            for sa in s.samples:
                for k, v in sa.blob_sizes.items():
                    _blob_sizes[k] += v
        return _blob_sizes

    @property
    def file_histogram(self):
        """Aggregate sample sizes by property date."""
        def histogram():
            return {'count': 0, 'size': 0, 'date': None}
        _file_histogram = defaultdict(histogram)

        for s in self.subjects:
            for sa in s.samples:
                for blob in sa.blobs.values():
                    time_created = str(blob['time_created'])
                    date_created = datetime.fromisoformat(time_created).date().isoformat()
                    _file_histogram[date_created]['count'] += 1
                    _file_histogram[date_created]['date'] = date_created
                    _file_histogram[date_created]['size'] += blob['size']
        return _file_histogram

    @property
    def files(self):
        """Aggregate sample sizes by type."""
        def f():
            return {'count': 0, 'size': 0, 'type': None}
        _files = defaultdict(f)

        for s in self.subjects:
            for sa in s.samples:
                for blob in sa.blobs.values():
                    # get extension
                    type = blob['name'].replace('.gz', '')
                    type = type.split('/')[-1].split('.')[-1]
                    _files[type]['count'] += 1
                    _files[type]['type'] = type.title()
                    _files[type]['size'] += blob['size']
        return _files

    def _get_entities(self, entity_name):
        """Return all entities in a workspace."""
        return get_entities(self.attributes.workspace.namespace, self.attributes.workspace.name, entity_name)

    @property
    def subject_property_name(self):
        """Return name of subject/participant entity, depending on schema."""
        subject_property_name = 'subject'
        if 'participant' in self.schemas.keys():
            subject_property_name = 'participant'
        return subject_property_name

    @property
    def name(self):
        """Return name of workspace."""
        return self.attributes.workspace.name

    @property
    def schemas(self):
        """Return schema for workspace."""
        if not self._schemas:
            self._schemas = get_schema(self.attributes.workspace.namespace, self.attributes.workspace.name)
        return self._schemas

    @property
    def subject_schema(self):
        """Return schema for workspace subject."""
        return self.schemas.get(self.subject_property_name, None)

    @property
    def sample_schema(self):
        """Return schema for workspace sample."""
        if 'sample' not in self._schemas:
            logging.debug(f"{self.id} - no schema? {self._schemas}")
            return None
        return self._schemas['sample']

    def __repr__(self):
        """Return attributes."""
        return str(self.attributes)

    @property
    def data_type(self):
        """Return data type or None."""
        try:
            return self.attributes.workspace.attributes['library:datatype']['items']
        except Exception as e:
            self._logger.debug(f"data_category {e}")
            return None

    @property
    def data_category(self):
        """Return data category or None."""
        try:
            return self.attributes.workspace.attributes['library:dataCategory']['items']
        except Exception as e:
            self._logger.debug(f"data_category {e}")
            return None

    @property
    def problems(self):
        """Flag all problems."""
        return AttrDict({
            'inconsistent_entityName': self.inconsistent_entityName is not None,
            'inconsistent_subject': self.inconsistent_subject is not None,
            'missing_blobs': self.missing_blobs is not None or len(self.blobs().keys()) == 0,
            'missing_samples': self.missing_samples is not None or len(self.samples) == 0,
            'missing_project_files': self.missing_project_files is not None,
            'missing_subjects': self.missing_subjects,
            'missing_schema': self.schemas is None or len(self.schemas.keys()) == 0,
            'missing_sequence': self.missing_sequence
        })

    @property
    def dashboard_view(self):
        """Format for portal view."""
        return AttrDict({
            'file_histogram': [h for h in self.file_histogram.values()],
            'files': [f for f in self.files.values()],
            'nodes': [
                {
                    "type": "Project",
                    "count": 1
                },
                {
                    "type": "Subject",
                    "count": len(self.subjects)
                },
                {
                    "type": "Samples",
                    # samples is a dict keyed by subject id, sum the len of each subject's sample list
                    "count": sum([len(sl) for sl in list(self.samples.values())])
                },
            ],
            'size': sum([f['size']for f in self.files.values()]),
            'project_id': self.name,
            'public': self.attributes['public'],
            'createdDate': self.attributes.workspace.createdDate,
            'lastModified': self.attributes.workspace.lastModified,
            'data_type': self.data_type,
            'data_category': self.data_category,
            'problems': self.problems
        })

    @property
    def investigator(self):
        """Deduce investigator name."""
        _investigator = self.attributes.workspace.attributes.get("library:datasetOwner", None)
        if _investigator == 'NA':
            return None
        return _investigator

    @property
    def accession(self):
        """Deduce accession."""
        return self.attributes.workspace.attributes.get("library:datasetVersion", None)

    @property
    def institute(self):
        """Deduce institute."""
        _institute = self.attributes.workspace.attributes.get("library:institute", None)
        if _institute and 'items' in _institute:
            return _institute['items'][0]
        return _institute

    @property
    def diseaseOntologyId(self):
        """Deduce disease."""
        _diseaseOntologyID = self.attributes.workspace.attributes.get('diseaseOntologyID', None)
        if not _diseaseOntologyID:
            _diseaseOntologyID = self.attributes.workspace.attributes.get('library:diseaseOntologyID', None)
        if _diseaseOntologyID:
            _diseaseOntologyID = _diseaseOntologyID.split('/')[-1].replace('_', ':')
        else:
            logging.debug(f"{self.id} missing diseaseOntologyID")
        return _diseaseOntologyID


def _project_files(w):
    """Deduce attributes that are files."""
    return {k: v for k, v in w['attributes'].items() if isinstance(v, str) and v.startswith('gs://')}


@memoize
def _bucket_contents(user_project, bucket_name):
    """Fetch bucket contents."""
    storage_client = storage.Client(project=user_project)
    project_blobs = {}
    project_bucket = storage_client.bucket(bucket_name, user_project=user_project)
    for b in list(project_bucket.list_blobs()):
        name = f"gs://{project_bucket.name}/{b.name}"
        project_blobs[name] = {'size': b.size, 'etag': b.etag, 'crc32c': b.crc32c, 'time_created': b.time_created, 'name': name}
    return project_blobs


class CMGWorkspace(Workspace):
    """Extend Workspace class."""

    def __init__(self, *args, **kwargs):
        """Call super."""
        super().__init__(*args, **kwargs)

    @property
    def investigator(self):
        """Deduce investigator name."""
        _investigator = self.attributes.workspace.attributes.get('study_pi', None)
        if not _investigator:
            _investigator = self.attributes.workspace.attributes.get("library:datasetOwner", None)
        return _investigator

    @property
    def accession(self):
        """Deduce accession."""
        _accession = super().accession
        if not _accession:
            _accession = self.attributes.workspace.attributes.get("study_accession", None)
        return _accession


class CCDGWorkspace(Workspace):
    """Extend Workspace class."""

    def __init__(self, *args, **kwargs):
        """Call super."""
        super().__init__(*args, **kwargs)


def workspace_factory(*args, **kwargs):
    """Return a specialized Workspace class instance."""
    name = args[0]['workspace']['name']
    if 'CCDG' in name.upper():
        return CCDGWorkspace(*args, **kwargs)
    if 'CMG' in name.upper():
        return CMGWorkspace(*args, **kwargs)
    return Workspace(*args, **kwargs)
    # if 'GTEX' in kwargs['workspace'].name.upper():
    #     return GTExSubject(*args, **kwargs)
    # if '1000G-HIGH-COVERAGE' in kwargs['workspace'].name.upper():
    #     return ThousandGenomesSubject(*args, **kwargs)
    # if 'ANVIL_EMERGE' in kwargs['workspace'].name.upper():
    #     return eMERGESUbject(*args, **kwargs)
    raise Exception('Not implemented')
