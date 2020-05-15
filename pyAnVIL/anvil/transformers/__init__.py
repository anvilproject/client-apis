import os
from attrdict import AttrDict
from anvil import terra
import firecloud.api as FAPI
import networkx as nx
import logging
from datetime import date, datetime
import sqlite3
import json

PROJECT_CACHE = sqlite3.connect('project_cache.sqlite')


def CREATE_PROJECT_CACHE_TABLE():
    cur = PROJECT_CACHE.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS projects (
        project_id text PRIMARY KEY,
        transformer_name text NOT NULL,
        json text NOT NULL
    );""")
    PROJECT_CACHE.commit()


# static
CREATE_PROJECT_CACHE_TABLE()


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


class BaseApp():
    """The base class for terra to cannonical graph transformers."""

    def __init__(self, project_pattern=None, program='anvil-datastorage', fapi=FAPI, user_project=None):
        """Initializes workspace"""
        assert project_pattern, 'Please set project_pattern'
        assert program, 'Please set program'
        assert user_project, 'Please set user_project'
        self.program = program
        self.project_pattern = project_pattern
        self.fapi = fapi
        self.projects = None
        self.user_project = user_project
        self.G = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self.samples = None
        self.reported_already = False


    def project_cache_get_projects(self):
        cur = PROJECT_CACHE.cursor()
        sql = f"SELECT project_id, json FROM projects where transformer_name='{self.__class__.__name__}'"
        cur.execute(sql)
        while True:
            row = cur.fetchone()
            if not row:
                break
            project = AttrDict(json.loads(row[1]))
            # recast children
            project.project_files = {k: AttrDict(file) for k, file in project.project_files.items()}
            project.participants = [AttrDict(p) for p in project.participants]
            project.samples = [AttrDict(s) for s in project.samples]

            yield project

    def project_cache_put(self, project_id, project):
        project_json = None
        try:
            cur = PROJECT_CACHE.cursor()
            project_json = json.dumps(project, default=json_serial)
            cur.execute(f"REPLACE into projects(project_id, transformer_name, json) values (?, ?, ?);", (project_id, self.__class__.__name__, project_json, ))
            PROJECT_CACHE.commit()
        except Exception:
            raise

    def is_blacklist(self, project_id):
        """Returns true if blacklisted"""
        return False

    def get_terra_projects(self):
        """Yield project with associated schema."""
        db_count = 0
        for p in self.project_cache_get_projects():
            db_count += 1
            yield p

        if db_count > 0:
            return

        projects = terra.get_projects([self.program], project_pattern=self.project_pattern, fapi=self.fapi, user_project=self.user_project)
        projects = [p for p in projects if not self.is_blacklist(p.project_id)]

        assert len(projects) > 0, f"Should have at least 1 project in {self.program} matching {self.project_pattern}"
        self.logger.info(f"projects={[project.project_id for project in projects]}")
        blob_sum = sum([len(p.blobs) for p in projects])
        if blob_sum > 0:
            self.logger.info(f'number of blobs in {self.project_pattern}: {blob_sum}')
        else:
            self.logger.warning(f'number of blobs in {self.project_pattern}: {blob_sum}')

        # add the project schema
        projects = [terra.get_project_schema(p, fapi=self.fapi) for p in projects]
        for p in projects:
            if len(p.schema.keys()) == 0:
                self.logger.warning(f'{p.project} missing schema, project will not be included.')
            elif 'participant' not in p.schema and 'subject' not in p.schema:
                self.logger.warning(f'{p.project} does not have "participant" or "subject", project will not be included. {p.schema.keys()}')
            elif 'participant' in p.schema and 'subject' in p.schema:
                self.logger.warning(f'{p.project} has "participant" and "subject", project will not be included. {p.schema.keys()}')
            else:
                # normalize the project_files
                if len(p.project_files.keys()) == 0:
                    self.logger.debug(f'{p.project} has no project_files.')
                else:
                    _project_files = {}
                    for key, path in p.project_files.items():
                        filename, file_type = self.file_type(path)
                        blob = p.blobs.get(path, None)
                        size = blob['size'] if blob else 0
                        if size == 0:
                            self.logger.debug(f"{p['project_id']} {path} has no blob!")
                        _project_files[key] = AttrDict({'path': path, 'type': file_type, 'size': size})
                    p.project_files = _project_files

                if 'participant' in p.schema:
                    participants = terra.get_entities(namespace=p.program, workspace=p.project, entity_name='participant', fapi=self.fapi)
                    _participants = []
                    for participant in participants:
                        attributes = participant.attributes
                        attributes.submitter_id = participant.name
                        attributes.project_id = p.project_id
                        attributes.submitter_id = participant.name
                        attributes.project_id = p.project_id
                        _participants.append(attributes)
                    p.participants = _participants
                    assert len(p.participants) == p.schema.participant.count, f"Retrieved participants entities count {len(participants)} did not match anticipated count in schema {p.schema.participant.count}"
                
                # normalize to participant
                if 'subject' in p.schema:
                    participants = terra.get_entities(namespace=p.program, workspace=p.project, entity_name='subject', fapi=self.fapi)
                    _participants = []
                    for participant in participants:
                        attributes = participant.attributes
                        attributes.submitter_id = participant.name
                        attributes.project_id = p.project_id
                        attributes.submitter_id = participant.name
                        attributes.project_id = p.project_id
                        _participants.append(attributes)
                    p.participants = _participants
                    assert len(p.participants) == p.schema.subject.count, f"Retrieved subjects entities count {len(participants)} did not match anticipated count in schema {p.schema.subject.count}"

                
                
                samples = terra.get_entities(namespace=p.program, workspace=p.project, entity_name='sample', fapi=self.fapi)
                _samples = []
                for sample in samples:
                    attributes = sample.attributes
                    attributes.project_id = p.project_id
                    attributes.submitter_id = self.sample_submitter_id(attributes)
                    attributes.files = self.identify_files(attributes, p.blobs)
                    _samples.append(attributes)
                p.samples = _samples
                assert len(p.samples) == p.schema.sample.count, f"Retrieved samples entities count {len(samples)} did not match anticipated count in schema {p.schema.sample.count}"

                if 'sequencing' in p.schema:
                    sequencing = terra.get_entities(namespace=p.program, workspace=p.project, entity_name='sequencing', fapi=self.fapi)
                    _sequencing = []
                    for sequence in sequencing:
                        attributes = sequence.attributes
                        attributes.project_id = p.project_id
                        attributes.submitter_id = sequence.name  # note labled 'sequencing_id' on terra UI !!
                        attributes.files = self.identify_files(attributes, p.blobs)
                        _sequencing.append(attributes)
                    p.sequencing = _sequencing
                    assert len(p.sequencing) == p.schema.sequencing.count, f"Retrieved sequencing entities count {len(samples)} did not match anticipated count in schema {p.schema.sequencing.count}"



                self.project_cache_put(p['project_id'], p)
                yield p
        return

    def get_terra_participants(self):
        """Returns generator with participants associated with projects."""
        for p in self.get_terra_projects():
            for participant in p.participants:
                yield participant
            # participants = terra.get_entities(namespace=p.program, workspace=p.project, entity_name='participant', fapi=self.fapi)
            # if 'participant' not in p.schema:
            #     self.logger.warning(f"? workspace: {p.project} schema: {p.schema}")
            #     continue
            # assert len(participants) == p.schema.participant.count, f"Retrieved participants entities count {len(participants)} did not match anticipated count in schema {p.schema.participant.count}"
            # for participant in participants:
            #     attributes = participant.attributes
            #     attributes.submitter_id = participant.name
            #     attributes.project_id = p.project_id
            #     yield attributes

    def sample_submitter_id(self, sample):
        """Creates a sample submitter_id."""
        return '{}/{}'.format(
            sample.project_id,
            sample.get('sample_alias', sample.get('collaborator_sample_id'))
        )

    def get_terra_samples(self):
        """Returns generator with samples associated with projects."""
        self.logger.info('get_terra_samples')
        for p in self.get_terra_projects():
            for s in p.samples:
                yield s

    def identify_files(self, sample, blobs):
        """Returns a dictionary of files associated with this sample."""
        files = {}
        for k, v in sample.items():
            if k not in ['cram', 'md5_path', 'crai_path', 'cram_path', 'bam', 'bam_path', 'ase_wasp_counts',
                         'ase_counts', 'cram_file', 'cram_index', 'wgs_cram_index', 'wgs_cram_file',
                         'wes_bam_index', 'wes_bam_file', 'ase_wasp_chrX_raw_counts', 'ase_chrX_raw_counts',
                         'crai_or_bai_path', 'cram_or_bam_path', 'bam_file', 'bam_index']:
                continue

            filename, file_type = self.file_type(v)
            if 'md5' not in file_type:
                blob = blobs.get(v, None)
                size = blob['size'] if blob else 0
                time_created = blob['time_created'] if blob else 0
                if size == 0:
                    self.logger.warning(f"identify_files {sample['project_id']} {v} has no blob!")
                files[k] = AttrDict({'path': v, 'type': file_type, 'size': size, 'time_created': time_created})
            else:
                for k, f in files.items():
                    if f.path == filename:
                        f.md5 = v
        return files

    # def file_md5(self, path):
    #     """Returns path if file is an md5 container, None otherwise"""
    #     filename, file_extension = os.path.splitext(path)
    #     if file_extension == '.md5':
    #         return path
    #     return None

    def file_type(self, path):
        """Returns filename, file_extension ignores gz extension"""
        filename, file_extension = os.path.splitext(path)
        if file_extension == '.gz':
            file_extension = filename.split('.')[-1]
        return filename, file_extension

    def is_file(self, path):
        """Returns True if path is in google storage, False otherwise"""
        if isinstance(path, str) and path.startswith('gs://'):
            return True
        return False

    def to_graph(self):
        """Creates a simple networkx graph."""
        if self.G:
            return self.G
        G = nx.MultiDiGraph()
        for project in self.get_terra_projects():
            G.add_node(project.project_id, label='Project', 
                project_id=project.project_id, public=project.public, 
                createdDate=project.createdDate, lastModified=project.lastModified, 
                data_type=project.data_type, data_category=project.data_category)
            for k, file in project.project_files.items():
                type = file.type.replace('.', '').capitalize()
                G.add_node(file.path, label=f'{type}File', project_id=project.project_id, size=file.size)
                G.add_edge(project.project_id, file.path, label=type)

        # start = datetime.datetime.now()
        for subject in self.get_terra_participants():
            # end = datetime.datetime.now()
            # print('got participant', end-start)
            # start = end
            assert subject.submitter_id, 'should have submitter_id'
            assert subject.project_id, 'should have project_id'
            G.add_node(subject.submitter_id, label='Subject', **subject)
            G.add_edge(subject.submitter_id, subject.project_id, label='member_of')
        # start = datetime.datetime.now()
        for sample in self.get_terra_samples():
            # end = datetime.datetime.now()
            # print('got sample', end-start)
            # start = end
            assert sample.submitter_id, 'should have submitter_id'
            assert sample.project_id, 'should have project_id'
            #assert sample.participant , f'should have participant {sample.keys()} {sample}'
            if not sample.participant:
                self.logger.warning(f"{sample.project_id}, {sample.submitter_id} missing participant")
                continue
            participant = sample.participant
            if not isinstance(participant, str):
                participant = participant['entityName']
            G.add_node(sample.submitter_id, label='Sample', project_id=sample.project_id)
            G.add_edge(participant, sample.submitter_id, label='drawn_from')
            for k, file in sample.files.items():
                file = AttrDict(file)
                type = file.type.replace('.', '').capitalize()
                G.add_node(file.path, label=f'{type}File', project_id=sample.project_id, size=file.size, time_created=file.time_created)
                G.add_edge(sample.submitter_id, file.path, label=type)
            # end = datetime.datetime.now()
            # print('added nodes', end-start)
            # start = end

        self.G = G
        return self.G

    def graph_node_counts(self):
        """Returns a dict of node counts by project/label."""
        counts = {}
        print('graph_node_counts', self.program, self.project_pattern)        
        graph = self.to_graph()
        for n in graph.nodes():
            if 'label' not in graph.nodes[n]:
                if len(graph.out_edges(n)) > 0:
                    for e in graph.out_edges(n):
                        for o in e:
                            print(graph[o])

            assert 'label' in graph.nodes[n], f'{n} has no label? {graph.nodes[n]} {graph.in_edges(n)} {graph.out_edges(n)}'
            label = graph.nodes[n]['label']
            assert 'project_id' in graph.nodes[n], f'{label}({n}) has no project_id?'
            project_id = graph.nodes[n]['project_id']
            node_size = graph.nodes[n].get('size', 0)
            project_counts = counts.get(project_id, {})
            label_counts = project_counts.get(label, 0) + 1
            project_size = project_counts.get('size', 0) + node_size
            project_counts[label] = label_counts
            project_counts['size'] = project_size
            if 'Project' == label:
                project_counts['public'] = graph.nodes[n]['public']
                project_counts['createdDate'] = graph.nodes[n]['createdDate']
                project_counts['lastModified'] = graph.nodes[n]['lastModified']
            counts[project_id] = project_counts
        return counts

    def graph_project_summary(self):
        """Returns a dict project details."""

        projects = {}
        print('graph_project_summary', self.program, self.project_pattern)

        graph = self.to_graph()
        for n in graph.nodes():
            assert 'label' in graph.nodes[n], f'{n} has no label?'
            label = graph.nodes[n]['label']
            assert 'project_id' in graph.nodes[n], f'{label}({n}) has no project_id?'
            project_id = graph.nodes[n]['project_id']
            node_size = graph.nodes[n].get('size', 0)
            time_created = graph.nodes[n].get('time_created', 0)
            project_counts = projects.get(project_id, {'file_histogram': {}, 'files': {}, 'nodes': {}, 'size': 0, 'public': False, 'project_id': None})
            if label.endswith('File'):
                file_counts = project_counts['files'].get(label, {})
                file_counts['type'] = label.replace('File', '')
                file_counts['size'] = file_counts.get('size', 0) + node_size
                file_counts['count'] = file_counts.get('count', 0) + 1
                project_counts['files'][label] = file_counts
                if not time_created == 0:
                    date_created = datetime.fromisoformat(time_created).date()
                    date_created = date_created.isoformat()
                    if date_created not in project_counts['file_histogram']:
                        project_counts['file_histogram'][date_created] = {'count': 0, 'size': 0}
                    project_counts['file_histogram'][date_created]['count'] += 1
                    project_counts['file_histogram'][date_created]['size'] += node_size
            else:
                node_counts = project_counts['nodes'].get(label, {})
                node_counts['type'] = label
                node_counts['count'] = node_counts.get('count', 0) + 1
                project_counts['nodes'][label] = node_counts
            project_size = project_counts.get('size', 0) + node_size
            project_counts['size'] = project_size
            if 'Project' == label:
                project_counts['public'] = graph.nodes[n]['public']
                project_counts['createdDate'] = graph.nodes[n]['createdDate']
                project_counts['lastModified'] = graph.nodes[n]['lastModified']
                project_counts['data_type'] = graph.nodes[n]['data_type']
                project_counts['data_category'] = graph.nodes[n]['data_category']

            projects[project_id] = project_counts

        # adjust file_histogram from {} to []
        for project_id, project_counts in projects.items():
            _file_histogram = []
            for k, v in project_counts['file_histogram'].items():
                v['date'] = k
                _file_histogram.append(v)
            project_counts['file_histogram'] = _file_histogram

        return projects


def strip_NA(v):
    """Cleans up."""
    if v in ['NA', '#N/A', 'N/A', 'n/a']:
        return None
    return v


def strip_0(v):
    """Cleans up."""
    if v in [0, '0']:
        return None
    return v


def strip_all(v):
    """Cleans up."""
    return strip_0(strip_NA(v))
